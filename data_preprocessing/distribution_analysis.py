import polars as pl
import os
import shutil
from pathlib import Path


_FILENAME = "world0_USonly_vendorcleaned.tsv"

def process_large_dataset(file_path: str):
    # 1. Setup Pathing
    input_path = Path(file_path)
    base_name = input_path.stem  # e.g., 'dataset' from 'dataset.tsv'
    results_dir = Path(f"{base_name}_results")

    # 2. Directory Cleanup and Creation
    if results_dir.exists() and results_dir.is_dir():
        print(f"Deleting existing directory: {results_dir}")
        shutil.rmtree(results_dir)
    
    results_dir.mkdir(parents=True, exist_ok=True)
    print(f"Created directory: {results_dir}")

    # 3. Initialize LazyFrame
    # We use scan_csv to process the 100GB file lazily
    lf = pl.scan_csv(
        file_path, 
        separator='\t', 
        infer_schema_length=10000,
        ignore_errors=True
    )

    available_cols = set(lf.collect_schema().names())

    # 4. Filter to US rows only
    print("Filtering to US rows...")
    if "add_country_code" in available_cols:
        lf = lf.filter(pl.col("add_country_code").str.to_lowercase() == "us")
    else:
        print("WARNING: add_country_code not found. Skipping US-only filter.")

    # 5. Get Total Row Count after filtering (needed for proportions)
    print("Calculating US-only row count...")
    total_rows = lf.select(pl.len()).collect(streaming=True).item()
    print(f"Total US rows to process: {total_rows:,}")

    if total_rows == 0:
        print("No US rows found after filtering. Exiting.")
        return

    # Define categorical columns to analyze
    # Note: Using 'vendor_src' as it was in your list (instead of 'vendor_flags')
    categories = [
        "class", "type", "addresstype", "vendor", "vendor_src", "mac_flags",
        "add_state", "add_county", "add_town"
    ]
    categories = [col for col in categories if col in available_cols]

    # 6. Process Categorical Distributions
    for col_name in categories:
        print(f"Processing distribution for: {col_name}...")
        
        output_filename = results_dir / f"{base_name}_{col_name}_results.txt"
        
        # Build the query
        query = (
            lf.group_by(col_name)
            .agg(pl.len().alias("count"))
            .with_columns(
                (pl.col("count") / total_rows).alias("proportion")
            )
            .sort("count", descending=True)
        )
        
        # Execute in streaming mode and write to file
        # .collect(streaming=True) is the key for 100GB+ files
        df_result = query.collect(streaming=True)
        df_result.write_csv(output_filename, separator='\t')
        print(f"   Saved to: {output_filename}")

    print("Processing proportion of 'yes' type per class...")
    output_filename = results_dir / f"{base_name}_yes_per_class_results.txt"
    class_totals = lf.group_by("class").agg(pl.len().alias("total_in_class"))
    yes_per_class = (
            lf.filter(pl.col("type") == "yes")
            .group_by("class")
            .agg(pl.len().alias("yes_count"))
        )

    yes_distribution = (
            class_totals.join(yes_per_class, on="class", how="left")
            .with_columns(
                pl.col("yes_count").fill_null(0),
                (pl.col("yes_count") / pl.col("total_in_class")).alias("yes_proportion")
                )
            .sort("yes_count", descending=True)
            .collect(streaming=True)

    )
    yes_distribution.write_csv(output_filename, separator='\t')
    print(f"   Saved to: {output_filename}")


    # 7. Process cross-tabs
    cross_tabs = [
        ("vendor", "type"),
        ("type", "addresstype"),
        ("vendor", "addresstype")
]

    for left_col, right_col in cross_tabs:
        if left_col not in available_cols or right_col not in available_cols:
            continue

        print(f"Processing cross-tab: {left_col} x {right_col}...")
        output_filename = results_dir / f"cross_{base_name}_{left_col}_x_{right_col}.txt"

        query = (
            lf.group_by([left_col, right_col])
            .agg(pl.len().alias("count"))
            .with_columns([
                (pl.col("count") / total_rows).alias("proportion_of_total"),
                (pl.col("count") / pl.col("count").sum().over(left_col)).alias(f"proportion_within_{left_col}"),
                (pl.col("count") / pl.col("count").sum().over(right_col)).alias(f"proportion_within_{right_col}")
            ])
            .sort("count", descending=True)
            .head(5000)
        )

        df_result = query.collect(streaming=True)
        df_result.write_csv(output_filename, separator='\t')
        print(f"   Saved to: {output_filename}")

    # 8. Process names repeating more than 5 times
    if "name" in available_cols:
        print("Processing names with count > 5...")
        name_output = results_dir / f"{base_name}_name_results.txt"

        name_query = (
            lf.group_by("name")
            .agg(pl.len().alias("count"))
            .filter(pl.col("count") > 5)
            .sort("count", descending=True)
        )

        df_names = name_query.collect(streaming=True)
        df_names.write_csv(name_output, separator='\t')
        print(f"   Saved to: {name_output}")

    print("\nAll tasks completed successfully.")

if __name__ == "__main__":
    # Replace with your actual filename
   # FILENAME = "joy1TESTfeb8_integrated_output.tsv" 
    
    if os.path.exists(_FILENAME):
        process_large_dataset(_FILENAME)
    else:
        print(f"File {_FILENAME} not found.")
