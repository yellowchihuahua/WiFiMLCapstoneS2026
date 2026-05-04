import csv
import os
import string
import datetime
import requests
import json
import re
import shutil
import concurrent.futures
import contextlib

_NUM_THREADS = 128
_TIMESTAMP_COLUMN = 0
_BSSID_COLUMN = 1
_LATLONG_COLUMN = 2
_ALTITUDE_COLUMN = 5

#_INPUT_FILENAME = "10000sampledata.tsv"
_INPUT_FILENAME = "2026-01-17-10-50-world.tsv_2.tsv" 
#_INPUT_FILENAME = "cn-bssids-2026-01-17-08-57.tsv" 

_TEMP_DIR = "world2_mar4_split_temp" #_INPUT_FILENAME + "_temp"
_OUTPUT_FILENAME = "world2_mar4_integrated_output.tsv"
_FAILEDGEO_FILENAME = "world2_mar4_failed_geo.tsv"

_OUI_FILENAME = "/share/ouiclean.txt" 

vendor_map = {}

def split_file_streaming(input_file, num_chunks=_NUM_THREADS):
    # Create file handles for the n chunks
    out_paths = [os.path.join(_TEMP_DIR,f"{input_file}_{i}.tsv") for i in range(num_chunks)]
    
    with contextlib.ExitStack() as stack:
        handles = [stack.enter_context(open(p, 'w', encoding='utf-8')) for p in out_paths]

        with open(input_file, 'r', encoding='utf-8', errors='replace') as f:
            header = None
            line_idx = 0
            
            for line in f:
                # 1. Handle the header
                if line.startswith("#"):
                    header = line
                    # Write header to all files
                    for h in handles:
                        h.write(header)
                    continue
                
                # 2. Distribute lines round-robin using modulo
                target_file_idx = line_idx % num_chunks
                handles[target_file_idx].write(line)
                line_idx += 1
                
                if line_idx % 100000 == 0:
                    print(f"Distributed {line_idx} lines...")
        
    return out_paths

def merge_files(output_chunks, final_output):
    with open(final_output, 'wb') as wfd: # Open in binary for speed
        for i, f in enumerate(output_chunks):
            with open(f, 'rb') as fd:
                # If it's not the first file, skip the header line
                if i > 0:
                    fd.readline() 
                shutil.copyfileobj(fd, wfd) # Efficiently streams the data

def load_vendor_map():
    global vendor_map
    with open(_OUI_FILENAME, 'r', encoding='utf-8') as f:
        for line in f:
            if '\t' in line:
                prefix, vendor = line.strip().split('\t', 1)
                vendor_map[prefix.upper()] = vendor
    return vendor_map

# INTEGRATED: MAC/OUI Utilities
oui_vendor_cache = {}  # key:"AA:BB:CC"->vendor string or "NaN"

def mac_flags_from_first_octet(first_octet: int) -> int: #Compute mac_flags
    """
    bit0 (1): multicast (I/G)
    bit1 (2): local (U/L)
    """
    multicast = 1 if (first_octet & 0x01) else 0
    local = 2 if (first_octet & 0x02) else 0
    return multicast | local

def flip_first_octet_bits(first_octet: int, flip_mask: int) -> int:
    return first_octet ^ flip_mask # Flip bits

def vendor_from_oui(oui:str) -> str: # Vendor lookup from OUI with caching
    cached = oui_vendor_cache.get(oui)
    if cached is not None:
        return cached
    
    # normalize to match ouiclean.txt format
    normalized_oui = oui.upper().replace(":", "-")

    vendor = vendor_map.get(normalized_oui, "NaN")

    oui_vendor_cache[oui] = vendor
    return vendor

def vendor_with_bit_flips(bssid: str):
    bssid = bssid.strip().upper().replace("-", ":")
    parts = bssid.split(":")
    if len(parts) < 3:
        return "NaN", 0, 0
    
    first_octet = int(parts[0], 16)
    mac_flags = mac_flags_from_first_octet(first_octet)

    def oui_from_octet(octet_val: int):
        return f"{octet_val:02X}:{parts[1]}:{parts[2]}"

    # 1. Direct match
    vendor = vendor_from_oui(oui_from_octet(first_octet))
    if vendor != "NaN":
        return vendor, mac_flags, 1

    # 2. Bit flips
    flip_options = [
        (1, 2),  # flip multicast
        (2, 3),  # flip local
        (3, 4),  # flip both
    ]

    for flip_mask, src_code in flip_options:
        new_octet = flip_first_octet_bits(first_octet, flip_mask)
        vendor = vendor_from_oui(oui_from_octet(new_octet))
        if vendor != "NaN":
            return vendor, mac_flags, src_code

    return "NaN", mac_flags, 0

# NETWORKING
def fetch_json(session, lat, lon):
    try:
        url = "http://localhost/reverse"
        params = {
            'lat': lat,
            'lon': lon,
            'format': 'json',
            'accept-language': 'en'
        }
        
        response = session.get(url, params=params, timeout=10)
        error_code = response.status_code

        # checks success
        if error_code == 200:
            return response.text, error_code 
        else:
            print(f"Error: HTTP {error_code} for lat={lat}, lon={lon}")
            return None, error_code
            
    except requests.exceptions.Timeout:
        print(f"Timeout for lat={lat}, lon={lon}")
        return None,None
    except requests.exceptions.RequestException as e:
        print(f"Request exception: {str(e)}")
        return None,None

# WORKER
def parse_tsv_worker(file_path, output_path, failed_geo_path):
    local_total_lines_cnt = 0
    local_failed_geoloc_cnt = 0
    local_failed_vendor_cnt = 0

    session = requests.Session() 
    
    # Open the TSV file
    with open(file_path, mode='r', newline='', encoding='utf-8') as file, \
        open(output_path, mode='w', newline = '', encoding='utf-8') as output, \
        open(failed_geo_path, mode='w', newline = '', encoding='utf-8') as failedgeo:
            writer = csv.writer(output, delimiter='\t')
            reader = csv.reader(file, delimiter='\t')
            geowriter = csv.writer(failedgeo, delimiter='\t')

            #write a header in the file
            writer.writerow(["# epochtime","bssid","lat","lon","altitude","vendor","mac_flags","vendor_src","place_id","osm_type","osm_id","class","type","place_rank","importance","addresstype","name","display_name","add_road","add_neighbourhood","add_town","add_county","add_state","add_ISO3166v14","add_postcode", "add_country", "add_country_code"])

            # Loop through each row in the reader
            for row in reader:
                if row[0].startswith("#"):
                    pass
                else:
                    local_total_lines_cnt += 1
                    epochtime = int(row[_TIMESTAMP_COLUMN])
                    bssid = row[_BSSID_COLUMN]
                    latlong = row[_LATLONG_COLUMN]
                    altitude = row[_ALTITUDE_COLUMN]

                    # INTEGRATED: get vendor with blit flips
                    vendor, mac_flags, vendor_src = vendor_with_bit_flips(bssid)
                    if vendor == "NaN":
                        local_failed_vendor_cnt += 1

                    # to be extracted from json:
                    place_id = -1
                    osm_type = ""
                    osm_id = ""
                    jclass = ""
                    jtype = ""
                    place_rank = -1
                    importance = -1.00
                    addresstype = ""
                    name = ""
                    display_name = ""
                    add_road = ""
                    add_neighbourhood = ""
                    add_town = ""
                    add_county = ""
                    add_state = ""
                    add_ISO3166lvl4 = ""
                    add_postcode = ""
                    add_country = ""
                    add_country_code = ""

                    #human_time = datetime.datetime.fromtimestamp(epochtime)
                    lat, lon = map( lambda x: float(x), latlong.split(',') )
    
                    #rowdata = [epochtime, bssid, lat, lon, altitude]
                    check_count = 0
                    error_msg = None
                    error_code = None
                    while (check_count <10):
                        jsonraw, status_code = fetch_json(session, lat, lon)
                    
                        if status_code == 200:
                            #no error
                            break
                        else: 
                            #http error
                            error_msg = f"ERROR: Failed to fetch JSON after 10 attempts at lat={lat}, lon={lon}. Error code HTTP {status_code}"
                            error_code = status_code

                    if(jsonraw):
                        try:
                            json_obj = json.loads(jsonraw)
               
                            #to be extracted from json:
                            place_id = json_obj.get("place_id")
                            osm_type = json_obj.get("osm_type")
                            osm_id = json_obj.get("osm_id")
                            jclass = json_obj.get("class")
                            jtype = json_obj.get("type")
                            place_rank = json_obj.get("place_rank")
                            importance = json_obj.get("importance")
                            addresstype = json_obj.get("addresstype")
                            name = json_obj.get("name")
                            display_name = json_obj.get("display_name")
                            add_road = json_obj.get("address",{}).get("road")
                            add_neighbourhood = json_obj.get("address",{}).get("neighbourhood")
                            add_town = json_obj.get("address",{}).get("town")
                            add_county = json_obj.get("address",{}).get("county")
                            add_state = json_obj.get("address",{}).get("state")
                            add_ISO3166lvl4 = json_obj.get("address",{}).get("ISO3166-2-lvl4")
                            add_postcode = json_obj.get("address",{}).get("postcode")
                            add_country = json_obj.get("address",{}).get("country")
                            add_country_code = json_obj.get("address",{}).get("country_code")

                            rowdata = [epochtime, bssid, lat, lon, altitude, vendor, mac_flags, vendor_src, place_id, osm_type, osm_id, jclass, jtype, place_rank, importance, addresstype, name, display_name, add_road, add_neighbourhood, add_town, add_county, add_state, add_ISO3166lvl4, add_postcode, add_country, add_country_code]
                            writer.writerow(rowdata)

                        except json.JSONDecodeError as e:
                            print(f"JSON Decode Error: {e}")
                    else:
                        geowriter.writerow(row + f"\t{error_code}") #write the original file's row + the error code at the end
                        failedgeo.flush()
                        #rowinto failedgeo
                        local_failed_geoloc_cnt = local_failed_geoloc_cnt + 1                  

                    

    session.close() #close tcp connect
    return local_total_lines_cnt, local_failed_geoloc_cnt, local_failed_vendor_cnt


def cleanup_temp_files(file_list):
    """
    Deletes a list of file paths from the file system.
    """
    print(f"Starting cleanup of {len(file_list)} temporary files...")
    for file_path in file_list:
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Deleted: {file_path}")
            else:
                print(f"Skipping: {file_path} (File not found)")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")


if __name__ == "__main__":
    num_threads = _NUM_THREADS
    load_vendor_map()

    if not os.path.exists(_TEMP_DIR):
        os.makedirs(_TEMP_DIR)

    print("Splitting large input file...")
    input_chunks = split_file_streaming(_INPUT_FILENAME, num_threads)
    output_chunks = [os.path.join(_TEMP_DIR,f"output_chunk{i}.tsv") for i in range(num_threads)]
    failedgeo_chunks = [os.path.join(_TEMP_DIR, f"failed_geo{i}.tsv") for i in range(num_threads)]


    print("Starting processing threads...")
    total_processed = 0
    total_failed_geo_count = 0
    total_failed_vendor_count = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Launch threads and collect 'future' objects
        futures = [executor.submit(parse_tsv_worker, inc, outc, failedgeo) 
            for inc, outc, failedgeo in zip(input_chunks, output_chunks, failedgeo_chunks)]
        
        for future in concurrent.futures.as_completed(futures):
            t_cnt, g_cnt, v_cnt = future.result()
            total_processed += t_cnt
            total_failed_geo_count += g_cnt
            total_failed_vendor_count += v_cnt
    
    print("Merging final file...")
    merge_files(output_chunks, _OUTPUT_FILENAME)
    merge_files(failedgeo_chunks, _FAILEDGEO_FILENAME)
    cleanup_temp_files(input_chunks)
    cleanup_temp_files(output_chunks)
    cleanup_temp_files(failedgeo_chunks)
    os.rmdir(_TEMP_DIR)

    print(f"Done. Processed {total_processed} lines.")
    print(f"Entries failed geolocation lookup: {total_failed_geo_count}, written in {_FAILEDGEO_FILENAME}")
    print(f"Entries failed vendor lookup: {total_failed_vendor_count}.")


