import re

def clean_mac_file(input_file, output_file):
    # Regex to match the MAC prefix (hex format) and the company name
    # Pattern: Matches XX-XX-XX followed by optional space and (hex), 
    # then captures everything after that until the end of the line.

    
    pattern = re.compile(r'^([0-9A-F]{2}-[0-9A-F]{2}-[0-9A-F]{2})\s+\(hex\)\s+(.*)')

    extracted_data = []

    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                match = pattern.match(line)
                if match:
                    mac_prefix = match.group(1)
                    company_name = match.group(2).strip()
                    extracted_data.append(f"{mac_prefix}\t{company_name}")

        # Write the cleaned data to the new file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(extracted_data))
            
        print(f"Success! Processed {len(extracted_data)} entries into {output_file}")

    except FileNotFoundError:
        print("Error: The input file was not found.")

# Usage
clean_mac_file('oui.txt', 'ouiclean.txt')
