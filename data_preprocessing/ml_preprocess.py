#!/usr/bin/env python3
"""Build ML-ready reverse-geocoded Wi-Fi dataset with labels.

Design principles:
- Label by class+type semantics (OSM key/value style).
- Export one training table plus light summaries.
"""

from __future__ import annotations

import argparse
import json
import re
import runpy
from pathlib import Path

import polars as pl


TRANSPORT_CLASSES = {"highway", "railway", "aeroway", "aerialway", "junction", "tunnel"}
TRANSPORT_TYPES = {
    "parking",
    "parking_entrance",
    "parking_space",
    "primary",
    "secondary",
    "tertiary",
    "unclassified",
    "service",
    "trunk",
    "motorway",
    "cycleway",
    "path",
    "footway",
    "pedestrian",
    "track",
    "bus_stop",
    "construction",
    "bridge",
    "pier",
    "terminal",
    "aerodrome",
    "apron",
    "fuel",
}

BUILDING_RESIDENTIAL_TYPES = {
    "house",
    "detached",
    "semidetached_house",
    "terrace",
    "apartments",
    "residential",
    "residential_duplex",
    "residential town homes",
    "dormitory",
    "static_caravan",
}
PLACE_RESIDENTIAL_TYPES = {
    "house",
    "isolated_dwelling",
    "hamlet",
    "village",
    "town",
    "suburb",
    "neighbourhood",
}
RESIDENTIAL_STREET_TYPES = {"residential", "living_street"}

CIVIC_EDU_TYPES = {"school", "university",
                   "college", "kindergarten", "library"}
CIVIC_HEALTH_TYPES = {"hospital", "clinic", "doctors",
                      "dentist", "pharmacy", "nursing_home", "chemist"}
CIVIC_WORSHIP_TYPES = {"place_of_worship", "church",
                       "mosque", "synagogue", "temple", "chapel"}
CIVIC_GOV_TYPES = {
    "government",
    "courthouse",
    "townhall",
    "police",
    "fire_station",
    "prison",
    "social_facility",
    "community_centre",
    "social_centre",
}
TRANSPORT_AMENITY_TYPES = {
    "bus_station",
    "bicycle_parking",
    "bicycle_rental",
    "charging_station",
    "car_rental",
    "ferry_terminal",
    "taxi",
    "motorcycle_parking",
}
TRANSPORT_EXTRA_BUILDING_TYPES = {"garage", "garages", "train_station", "transportation", "carport"}
CIVIC_INFRA_AMENITY_TYPES = {
    "shelter",
    "waste_basket",
    "post_box",
    "letter_box",
    "toilets",
    "vending_machine",
    "recycling",
    "drinking_water",
    "public_bookcase",
    "bbq",
    "parcel_locker",
    "animal_shelter",
}
RESIDENTIAL_EXTRA_BUILDING_TYPES = {"bungalow"}
CIVIC_EXTRA_BUILDING_TYPES = {"public", "civic", "government", "fire_station", "public_building"}
COMMERCIAL_RECREATION_AMENITY_TYPES = {
    "bar",
    "pub",
    "casino",
    "nightclub",
    "food_court",
    "ice_cream",
    "marketplace",
    "studio",
    "veterinary",
}
COMMERCIAL_RECREATION_TOURISM_TYPES = {"zoo", "gallery", "picnic_site", "guest_house"}
CIVIC_EXTRA_TYPES = {
    "childcare",
    "research_institute",
    "conference_centre",
    "events_venue",
    "grave_yard",
    "driving_school",
    "music_school",
    "prep_school",
    "language_school",
    "dancing_school",
    "training",
    "polling_station",
    "public_building",
    "public_facility",
    "retirement_home",
    "student_accommodation",
    "student_accomodation",
    "monastery",
}
RESIDENTIAL_EXTRA_AMENITY_TYPES = {"trailer_park"}
INDUSTRIAL_EXTRA_BUILDING_TYPES = {"data_center", "manufacture"}
INDUSTRIAL_EXTRA_MAN_MADE_TYPES = {"tower"}
INDUSTRIAL_AMENITY_TYPES = {"loading_dock", "waste_transfer_station", "vehicle_inspection", "weighbridge"}
INDUSTRIAL_LANDUSE_TYPES = {"industrial", "commercial", "farmyard", "construction", "quarry"}
COMMERCIAL_RECREATION_TOURISM_TYPES = COMMERCIAL_RECREATION_TOURISM_TYPES | {
    "hostel",
    "chalet",
    "aquarium",
    "camp_pitch",
    "viewpoint",
}
OFFICE_CIVIC_TYPES = {
    "government",
    "diplomatic",
    "ngo",
    "charity",
    "foundation",
    "union",
    "association",
    "political_party",
    "quango",
    "administrative",
    "educational_institution",
    "university",
    "school",
    "research",
    "religion",
    "medical",
    "healthcare",
    "physician",
    "therapist",
}
OFFICE_INDUSTRIAL_TYPES = {
    "energy_supplier",
    "water_utility",
    "telecommunication",
    "logistics",
    "logistics_service",
    "construction_company",
    "construction",
    "engineer",
    "engineering",
    "hvac",
    "forestry",
    "transport",
    "moving_company",
}

INDUSTRIAL_TYPES = {"industrial", "warehouse",
                    "works", "factory", "hangar", "water_tower"}
RECREATION_TYPES = {
    "golf_course",
    "pitch",
    "playground",
    "sports_centre",
    "fitness_centre",
    "stadium",
    "marina",
    "dog_park",
    "resort",
    "museum",
    "theatre",
    "cinema",
    "park",
    "track",
    "swimming_pool",
}
HOSPITALITY_TYPES = {"hotel", "motel", "resort",
                     "camp_site", "caravan_site", "apartment"}
PUBLIC_SAFETY_TYPES = {"police", "fire_station"}
JUSTICE_GOV_TYPES = {"courthouse", "townhall", "government"}

# In strict mode these raw columns are dropped from ML export anyway.
# Dropping them early reduces scan/compute pressure on very large TSVs.
STRICT_EARLY_DROP_COLS = {
    "name",
    "display_name",
    "add_road",
    "add_neighbourhood",
    "add_town",
    "add_county",
    "add_postcode",
    "add_country",
    "add_ISO3166v14",
    "bssid",
    "place_id",
    "osm_id"
}
DEFAULT_INFER_SCHEMA_LENGTH = 2000
PARQUET_ROW_GROUP_SIZE = 8_000
# Strict mode: final ML feature columns (targets are appended separately).
STRICT_OUTPUT_FEATURE_COLS = {
    "timestamp",
    "channel",
    "horizontal_accuracy",
    "vertical_accuracy",
    "altitude",
    "lat",
    "lon",
    "place_rank",
    "importance",
    "vendor_clean",
    "requested",
    "add_state",
}
# Columns required to build labels correctly.
LABEL_BUILD_INPUT_COLS = {
    "class",
    "type",
    "addresstype",
    "add_country_code",
    "place_rank",
    "importance",
    "vendor",
}
REQUIRED_TARGET_COLS = {
    "label_target_main",
    "label_sensitive_binary",
    "label_sensitive_subtype",
}
STRICT_INPUT_KEEP_COLS = LABEL_BUILD_INPUT_COLS | STRICT_OUTPUT_FEATURE_COLS
BROAD_EXCLUDE_COLS = {
    # Semantic leakage / helper columns.
    "class",
    "type",
    "addresstype",
    "add_country_code",
    "_class_lc",
    "_type_lc",
    "_addresstype_lc",
    "_country_code_lc",
    "class_type_key",
    # High-entropy identifiers that are not useful as direct model features.
    "bssid",
    "place_id",
    "osm_id",
}
CROSSTAB_DEFAULT_PAIRS = [
    ("vendor_clean", "label_target_main"),
    ("vendor_clean", "label_sensitive_binary"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Preprocess reverse-geocoded Wi-Fi TSV into ML-ready labeled data.")
    p.add_argument("--input", required=True, help="Path to integrated TSV.")
    p.add_argument("--output-dir", default=None,
                   help="Default: <input_stem>_mlprep")
    p.add_argument("--country-code", default="us",
                   help="Filter by country code. Use '' to disable.")
    p.add_argument(
        "--yes-policy", choices=["unknown", "weak_class"], default="unknown")
    p.add_argument("--drop-unknown-target", action="store_true",
                   help="Drop rows with label_target_main == unknown in ML output.")
    p.add_argument("--feature-mode",
                   choices=["broad", "strict"], default="strict")
    p.add_argument("--export-flat",
                   choices=["none", "csv", "tsv"], default="none")
    p.add_argument(
        "--sample-rows",
        type=int,
        default=200,
        help="Write a small readable TSV sample (first N rows) from ML parquet; set 0 to disable.",
    )
    p.add_argument("--write-full", action="store_true",
                   help="Also export <stem>_labeled_full.parquet (off by default).")
    p.add_argument(
        "--emit-sensitive-coarse",
        action="store_true",
        help=(
            "Also emit label_sensitive_subtype_coarse with fewer privacy-risk subtypes "
            "(non_sensitive/social_sensitive/security_sensitive)."
        ),
    )
    p.add_argument(
        "--emit-crosstabs",
        action="store_true",
        help="Write exploratory crosstabs under <output_dir>/crosstabs.",
    )
    return p.parse_args()


def _norm_text(col: str) -> pl.Expr:
    return (
        pl.col(col)
        .cast(pl.Utf8, strict=False)
        .fill_null("")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .str.to_lowercase()
    )


def _safe_norm_column(schema_names: set[str], source_col: str, alias_col: str) -> pl.Expr:
    if source_col in schema_names:
        return _norm_text(source_col).alias(alias_col)
    return pl.lit("").alias(alias_col)


def _type_has_any(values: set[str]) -> pl.Expr:
    # Regex token-match avoids per-row list allocations from split/list.contains.
    escaped = "|".join(re.escape(v) for v in sorted(values))
    pattern = rf"(^|;)({escaped})(;|$)"
    return pl.col("_type_lc").str.contains(pattern)


def _invalid_row_expr() -> pl.Expr:
    cls = pl.col("_class_lc")
    typ = pl.col("_type_lc")
    return (cls == "") | (cls == "nan") | (typ == "") | (typ == "nan")


def _load_vendor_alias_map(base_dir: Path) -> dict[str, str]:
    # Make sure to have vendor-normalization.py in the same directory as the input TSV
    vn_path = base_dir / "vendor-normalization.py"
    if not vn_path.exists():
        return {}

    ns = runpy.run_path(str(vn_path))
    builder = ns.get("build_vendor_alias_map")
    if callable(builder):
        return builder()

    vendor_map = ns.get("VENDOR_MAP", {})
    normalize = ns.get("normalize_vendor_name",
                       lambda s: " ".join(str(s).strip().lower().split()))
    alias_map: dict[str, str] = {}
    for canonical, variants in vendor_map.items():
        c = normalize(canonical)
        if c:
            alias_map[c] = canonical
        for v in variants:
            vv = normalize(v)
            if vv:
                alias_map[vv] = canonical
    return alias_map


def _vendor_clean_expr(alias_map: dict[str, str]) -> pl.Expr:
    vendor_norm = _norm_text("vendor")
    mapped = vendor_norm.replace_strict(alias_map, default=None)
    return (
        pl.when((vendor_norm == "") | (vendor_norm == "nan"))
        .then(pl.lit("NaN"))
        .otherwise(pl.coalesce([mapped, pl.col("vendor").cast(pl.Utf8, strict=False).fill_null("").str.strip_chars()]))
        .alias("vendor_clean")
    )


def _target_label_expr(yes_policy: str) -> pl.Expr:
    cls = pl.col("_class_lc")
    typ = pl.col("_type_lc")
    adr = pl.col("_addresstype_lc")

    missing = (cls == "") | (typ == "") | (typ == "nan")
    is_yes = typ == "yes"
    has_residential_token = _type_has_any(BUILDING_RESIDENTIAL_TYPES | {"residential"})
    has_industrial_token = _type_has_any(
        INDUSTRIAL_TYPES
        | INDUSTRIAL_EXTRA_BUILDING_TYPES
        | {"industrial", "warehouse", "factory", "works", "manufacture", "data_center"}
    )
    is_office_civic = (cls == "office") & typ.is_in(list(OFFICE_CIVIC_TYPES))
    is_office_industrial = (cls == "office") & typ.is_in(
        list(OFFICE_INDUSTRIAL_TYPES))

    yes_label = (
        pl.when(cls.is_in(list(TRANSPORT_CLASSES)) | adr.is_in(
            ["road", "highway", "railway", "aeroway"]))
        .then(pl.lit("transport"))
        .when(cls.is_in(["shop", "office", "tourism"]))
        .then(pl.lit("commercial_recreation"))
        .when(cls == "leisure")
        .then(pl.lit("commercial_recreation"))
        .otherwise(pl.lit("unknown"))
    )

    is_residential_place = (
        ((cls == "building") & (typ.is_in(list(BUILDING_RESIDENTIAL_TYPES | RESIDENTIAL_EXTRA_BUILDING_TYPES)) | has_residential_token))
        | ((cls == "place") & typ.is_in(list(PLACE_RESIDENTIAL_TYPES)))
        | ((cls == "landuse") & (typ == "residential"))
        | ((cls == "amenity") & typ.is_in(list(RESIDENTIAL_EXTRA_AMENITY_TYPES)))
    )

    is_residential_street = (cls == "highway") & typ.is_in(
        list(RESIDENTIAL_STREET_TYPES))

    is_civic = (
        ((cls == "amenity") & typ.is_in(list(CIVIC_EDU_TYPES |
         CIVIC_HEALTH_TYPES | CIVIC_WORSHIP_TYPES | CIVIC_GOV_TYPES | CIVIC_EXTRA_TYPES | CIVIC_INFRA_AMENITY_TYPES)))
        | (cls == "healthcare")
        | ((cls == "office") & (typ == "government"))
        | is_office_civic
        | (cls == "military")
        | (cls == "emergency")
        | ((cls == "boundary") & (typ == "administrative"))
        | ((cls == "building") & typ.is_in(["church", "hospital", "school", "university", "college", "kindergarten"] + list(CIVIC_EXTRA_BUILDING_TYPES)))
    )

    is_transport = (
        cls.is_in(list(TRANSPORT_CLASSES))
        | adr.is_in(["road", "highway", "railway", "aeroway"])
        | typ.is_in(list(TRANSPORT_TYPES))
        | ((cls == "amenity") & typ.is_in(list(TRANSPORT_AMENITY_TYPES)))
        | ((cls == "building") & typ.is_in(list(TRANSPORT_EXTRA_BUILDING_TYPES)))
        | ((cls == "man_made") & (typ == "tunnel"))
    )
    is_transport = is_transport & (~is_residential_street)

    is_industrial = (
        ((cls == "building") & (typ.is_in(list(INDUSTRIAL_TYPES | INDUSTRIAL_EXTRA_BUILDING_TYPES)) | has_industrial_token))
        | ((cls == "landuse") & typ.is_in(list(INDUSTRIAL_LANDUSE_TYPES)))
        | ((cls == "man_made") & typ.is_in(["works", "water_tower"] + list(INDUSTRIAL_EXTRA_MAN_MADE_TYPES)))
        | ((cls == "amenity") & typ.is_in(list(INDUSTRIAL_AMENITY_TYPES)))
        | is_office_industrial
        | (cls == "craft")
    )

    is_recreation = (
        (cls == "leisure")
        | ((cls == "tourism") & typ.is_in(["museum", "attraction", "theme_park", "artwork", "camp_site", "caravan_site", "resort"]))
        | ((cls == "amenity") & typ.is_in(["theatre", "cinema", "arts_centre", "fountain", "bench"]))
        | typ.is_in(list(RECREATION_TYPES))
    )

    is_commercial = (
        (cls == "shop")
        | (cls == "club")
        | (cls == "historic")
        | (cls == "information")
        | (cls == "natural")
        | ((cls == "office") & (~typ.is_in(list(OFFICE_CIVIC_TYPES | OFFICE_INDUSTRIAL_TYPES | {"government"}))))
        | ((cls == "amenity") & typ.is_in(list(COMMERCIAL_RECREATION_AMENITY_TYPES)))
        | ((cls == "tourism") & typ.is_in(list(HOSPITALITY_TYPES)))
        | ((cls == "tourism") & typ.is_in(list(COMMERCIAL_RECREATION_TOURISM_TYPES)))
        | ((cls == "building") & typ.is_in(["commercial", "retail", "office", "hotel"]))
        | ((cls == "amenity") & typ.is_in(["restaurant", "fast_food", "cafe", "bank", "car_wash", "fuel", "post_office", "post_depot"]))
    )

    return (
        pl.when(missing).then(pl.lit("unknown"))
        .when(is_yes).then(yes_label if yes_policy == "weak_class" else pl.lit("unknown"))
        .when(is_residential_street).then(pl.lit("residential_street"))
        .when(is_residential_place).then(pl.lit("residential_place"))
        .when(is_civic).then(pl.lit("civic_service"))
        .when(is_transport).then(pl.lit("transport"))
        .when(is_industrial).then(pl.lit("other"))
        .when(is_recreation | is_commercial).then(pl.lit("commercial_recreation"))
        .otherwise(pl.lit("other"))
        .alias("label_target_main")
    )


def _sensitive_exprs() -> list[pl.Expr]:
    cls = pl.col("_class_lc")
    typ = pl.col("_type_lc")

    is_education = (cls == "amenity") & typ.is_in(list(CIVIC_EDU_TYPES))
    is_healthcare = ((cls == "amenity") & typ.is_in(
        list(CIVIC_HEALTH_TYPES)) | (cls == "healthcare"))
    is_worship = ((cls == "amenity") & (typ == "place_of_worship")) | (
        (cls == "building") & typ.is_in(["church", "mosque", "synagogue", "temple", "chapel"]))
    is_public_safety = (
        ((cls == "amenity") & typ.is_in(list(PUBLIC_SAFETY_TYPES)))
        | (cls == "emergency")
    )
    is_justice_governance = (
        ((cls == "amenity") & typ.is_in(list(JUSTICE_GOV_TYPES)))
        | ((cls == "office") & (typ == "government"))
    )
    is_custodial = (cls == "amenity") & (typ == "prison")
    is_military = cls == "military"

    is_sensitive = (
        is_education
        | is_healthcare
        | is_worship
        | is_public_safety
        | is_justice_governance
        | is_custodial
        | is_military
    )
    sensitive_flag = is_sensitive.cast(pl.Int8).alias("label_sensitive_binary")

    subtype = (
        pl.when(~is_sensitive).then(
            pl.lit("non_sensitive"))
        .when(is_education).then(pl.lit("education"))
        .when(is_healthcare).then(pl.lit("healthcare"))
        .when(is_worship).then(pl.lit("worship"))
        .when(is_public_safety).then(pl.lit("public_safety"))
        .when(is_justice_governance).then(pl.lit("justice_governance"))
        .when(is_custodial).then(pl.lit("custodial_security"))
        .when(is_military).then(pl.lit("military_security"))
        .otherwise(pl.lit("non_sensitive"))
        .alias("label_sensitive_subtype")
    )
    return [sensitive_flag, subtype]


def _sensitive_coarse_expr() -> pl.Expr:
    subtype = pl.col("label_sensitive_subtype")
    return (
        pl.when(subtype == "non_sensitive").then(pl.lit("non_sensitive"))
        .when(subtype.is_in(["education", "healthcare", "worship"]))
        .then(pl.lit("social_sensitive"))
        .when(subtype.is_in(["public_safety", "justice_governance", "custodial_security", "military_security"]))
        .then(pl.lit("security_sensitive"))
        .otherwise(pl.lit("non_sensitive"))
        .alias("label_sensitive_subtype_coarse")
    )


def _feature_columns(columns: list[str], feature_mode: str) -> list[str]:
    if feature_mode == "strict":
        keep = [c for c in columns if c in STRICT_OUTPUT_FEATURE_COLS]
        for t in REQUIRED_TARGET_COLS:
            if t not in keep and t in columns:
                keep.append(t)
        if "label_sensitive_subtype_coarse" in columns:
            keep.append("label_sensitive_subtype_coarse")
        return keep

    keep = [c for c in columns if c not in BROAD_EXCLUDE_COLS]

    # Ensure targets exist in training export.
    for t in REQUIRED_TARGET_COLS:
        if t not in keep and t in columns:
            keep.append(t)
    if "label_sensitive_subtype_coarse" in columns and "label_sensitive_subtype_coarse" not in keep:
        keep.append("label_sensitive_subtype_coarse")
    return keep


def _sink_parquet(lf: pl.LazyFrame, path: Path) -> None:
    try:
        lf.sink_parquet(
            str(path),
            compression="snappy",
            statistics=False,
            row_group_size=PARQUET_ROW_GROUP_SIZE,
            maintain_order=False,
            engine="streaming",
        )
    except Exception as e:
        # Avoid eager fallback on huge files; it can trigger OOM kills.
        raise RuntimeError(
            f"sink_parquet failed for {path}; aborting to avoid non-streaming fallback."
        ) from e


def _sink_delimited(lf: pl.LazyFrame, path: Path, sep: str) -> None:
    try:
        lf.sink_csv(str(path), separator=sep)
    except Exception:
        lf.collect(streaming=True).write_csv(path, separator=sep)


def _configure_streaming_runtime() -> None:
    # Lower chunk size can reduce peak memory on very large streaming jobs.
    try:
        setter = getattr(pl.Config, "set_streaming_chunk_size", None)
        if callable(setter):
            setter(4_000)
    except Exception:
        pass


def _apply_row_filters(lf: pl.LazyFrame, country_code: str) -> pl.LazyFrame:
    if country_code:
        lf = lf.filter(pl.col("_country_code_lc") == country_code.lower())
    return lf.filter(~_invalid_row_expr())


def _required_crosstab_source_cols() -> set[str]:
    cols: set[str] = set()
    for left_col, right_col in CROSSTAB_DEFAULT_PAIRS:
        cols.add(left_col)
        cols.add(right_col)
    return cols


def _write_crosstabs(
    lf: pl.LazyFrame,
    out_dir: Path,
    base_name: str,
) -> None:
    available_cols = set(lf.collect_schema().names())
    total_rows = lf.select(pl.len().alias("n")).collect(engine="streaming").item()
    if total_rows == 0:
        return

    for left_col, right_col in CROSSTAB_DEFAULT_PAIRS:
        needed = {left_col, right_col}
        if not needed.issubset(available_cols):
            continue

        print(f"Processing cross-tab: {left_col} x {right_col}...")
        output_filename = out_dir / f"cross_{base_name}_{left_col}_x_{right_col}.tsv"
        group_cols = [left_col, right_col]
        query = (
            lf.group_by(group_cols)
            .agg(pl.len().alias("count"))
            .with_columns([
                (pl.col("count") / total_rows).alias("proportion_of_total"),
                (pl.col("count") / pl.col("count").sum().over(left_col)).alias(f"proportion_within_{left_col}"),
                (pl.col("count") / pl.col("count").sum().over(right_col)).alias(f"proportion_within_{right_col}"),
            ])
            .sort("count", descending=True)
        )

        query.collect(engine="streaming").write_csv(output_filename, separator="\t")


def main() -> None:
    args = parse_args()
    _configure_streaming_runtime()
    in_path = Path(args.input)
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    out_dir = Path(args.output_dir) if args.output_dir else Path(
        f"{in_path.stem}_mlprep")
    out_dir.mkdir(parents=True, exist_ok=True)
    do_crosstabs = args.emit_crosstabs

    alias_map = _load_vendor_alias_map(in_path.parent)

    print("Stage 1/4: scanning TSV lazily...")
    lf = pl.scan_csv(
        str(in_path),
        separator="\t",
        infer_schema=False,
        infer_schema_length=DEFAULT_INFER_SCHEMA_LENGTH,
        ignore_errors=True,
        low_memory=True,
        cache=False,
        rechunk=False,
        truncate_ragged_lines=True,
    )

    schema_names = set(lf.collect_schema().names())
    if "# epochtime" in schema_names and "epochtime" not in schema_names:
        lf = lf.rename({"# epochtime": "epochtime"})
        schema_names.discard("# epochtime")
        schema_names.add("epochtime")

    # Strict mode optimization: prune known-unused heavy columns early.
    if args.feature_mode == "strict":
        early_drop = sorted(c for c in STRICT_EARLY_DROP_COLS if c in schema_names)
        if early_drop:
            lf = lf.drop(early_drop)
            schema_names = schema_names.difference(early_drop)
        # Keep only columns needed for labels + strict features before heavy transforms.
        strict_input_keep = sorted(
            c for c in STRICT_INPUT_KEEP_COLS if c in schema_names
        )
        if strict_input_keep:
            lf = lf.select(strict_input_keep)
            schema_names = set(strict_input_keep)

    lf = lf.with_columns(
        [
            _safe_norm_column(schema_names, "class", "_class_lc"),
            _safe_norm_column(schema_names, "type", "_type_lc"),
            _safe_norm_column(schema_names, "addresstype", "_addresstype_lc"),
            _safe_norm_column(
                schema_names, "add_country_code", "_country_code_lc"),
            pl.col("place_rank").cast(pl.Int64, strict=False).alias(
                "place_rank") if "place_rank" in schema_names else pl.lit(None).alias("place_rank"),
            pl.col("importance").cast(pl.Float64, strict=False).alias(
                "importance") if "importance" in schema_names else pl.lit(None).alias("importance"),
        ]
    )

    lf = _apply_row_filters(lf, args.country_code)

    if "vendor" in schema_names:
        lf = lf.with_columns(_vendor_clean_expr(alias_map))
    else:
        lf = lf.with_columns(pl.lit("NaN").alias("vendor_clean"))
    # Raw vendor is no longer needed after normalization in strict mode.
    if args.feature_mode == "strict" and "vendor" in schema_names:
        lf = lf.drop("vendor")
        schema_names.discard("vendor")

    lf = lf.with_columns([
        _target_label_expr(args.yes_policy),
        *_sensitive_exprs(),
    ])
    if args.emit_sensitive_coarse:
        lf = lf.with_columns(_sensitive_coarse_expr())
    columns = list(schema_names)
    for c in [
        "_class_lc",
        "_type_lc",
        "_addresstype_lc",
        "_country_code_lc",
        "place_rank",
        "importance",
        "vendor_clean",
        "label_target_main",
        "label_sensitive_binary",
        "label_sensitive_subtype",
    ]:
        if c not in columns:
            columns.append(c)
    if args.emit_sensitive_coarse and "label_sensitive_subtype_coarse" not in columns:
        columns.append("label_sensitive_subtype_coarse")
    feature_cols = _feature_columns(columns, args.feature_mode)

    ml_lf = lf.select(feature_cols)
    if args.drop_unknown_target:
        ml_lf = ml_lf.filter(pl.col("label_target_main") != "unknown")

    ml_parquet = out_dir / f"{in_path.stem}_ml_dataset.parquet"
    print("Stage 2/4: writing ML parquet (streaming sink)...")
    _sink_parquet(ml_lf, ml_parquet)
    full_parquet = out_dir / f"{in_path.stem}_labeled_full.parquet"
    if args.write_full:
        _sink_parquet(lf, full_parquet)

    if args.export_flat == "csv":
        _sink_delimited(ml_lf, out_dir / f"{in_path.stem}_ml_dataset.csv", ",")
    elif args.export_flat == "tsv":
        _sink_delimited(ml_lf, out_dir /
                        f"{in_path.stem}_ml_dataset.tsv", "\t")

    sample_tsv = None
    if args.sample_rows > 0:
        sample_tsv = out_dir / f"{in_path.stem}_ml_dataset_sample.tsv"
        print("Stage 3/4: writing small sample TSV...")
        (
            pl.scan_parquet(str(ml_parquet))
            .limit(args.sample_rows)
            .collect(engine="streaming")
            .write_csv(sample_tsv, separator="\t")
        )

    print("Stage 4/4: writing summary TSVs...")
    summary_lf = pl.scan_parquet(str(ml_parquet))
    total = summary_lf.select(pl.len().alias("n")).collect(
        engine="streaming").item()

    (
        summary_lf.group_by("label_target_main")
        .agg(pl.len().alias("count"))
        .with_columns((pl.col("count") / total).alias("proportion"))
        .sort("count", descending=True)
        .collect(engine="streaming")
        .write_csv(out_dir / "label_target_main_summary.tsv", separator="\t")
    )

    (
        summary_lf.group_by("label_sensitive_binary")
        .agg(pl.len().alias("count"))
        .with_columns((pl.col("count") / total).alias("proportion"))
        .sort("count", descending=True)
        .collect(engine="streaming")
        .write_csv(out_dir / "label_sensitive_binary_summary.tsv", separator="\t")
    )

    (
        summary_lf.group_by("label_sensitive_subtype")
        .agg(pl.len().alias("count"))
        .with_columns((pl.col("count") / total).alias("proportion"))
        .sort("count", descending=True)
        .collect(engine="streaming")
        .write_csv(out_dir / "label_sensitive_subtype_summary.tsv", separator="\t")
    )

    if "label_sensitive_subtype_coarse" in columns:
        (
            summary_lf.group_by("label_sensitive_subtype_coarse")
            .agg(pl.len().alias("count"))
            .with_columns((pl.col("count") / total).alias("proportion"))
            .sort("count", descending=True)
            .collect(engine="streaming")
            .write_csv(out_dir / "label_sensitive_subtype_coarse_summary.tsv", separator="\t")
        )

    if do_crosstabs:
        crosstab_dir = out_dir / "crosstabs"
        crosstab_dir.mkdir(parents=True, exist_ok=True)
        crosstab_source = out_dir / f"{in_path.stem}_crosstab_source.parquet"
        crosstab_needed = _required_crosstab_source_cols()
        crosstab_cols = sorted(c for c in crosstab_needed if c in columns)
        try:
            _sink_parquet(lf.select(crosstab_cols), crosstab_source)
            _write_crosstabs(
                pl.scan_parquet(str(crosstab_source)),
                crosstab_dir,
                in_path.stem,
            )
        finally:
            if crosstab_source.exists():
                crosstab_source.unlink()

    metadata = {
        "input": str(in_path),
        "output_dir": str(out_dir),
        "country_code_filter": args.country_code.lower() if args.country_code else None,
        "yes_policy": args.yes_policy,
        "feature_mode": args.feature_mode,
        "drop_unknown_target": args.drop_unknown_target,
        "drop_invalid_rows": True,
        "emit_sensitive_coarse": args.emit_sensitive_coarse,
        "emit_crosstabs": do_crosstabs,
        "summary_source": "ml_parquet",
        "scan_cache": False,
        "scan_low_memory": True,
        "infer_schema_length": DEFAULT_INFER_SCHEMA_LENGTH,
        "vendor_alias_count": len(alias_map),
        "outputs": {
            "labeled_full_parquet": str(full_parquet) if args.write_full else None,
            "ml_dataset_parquet": str(ml_parquet),
            "flat_export": args.export_flat,
            "sample_tsv": str(sample_tsv) if sample_tsv else None,
        },
        "labels": {
            "target_main": [
                "residential_place",
                "residential_street",
                "transport",
                "commercial_recreation",
                "civic_service",
                "other",
                "unknown",
            ],
            "sensitive_binary": [0, 1],
            "sensitive_subtype": [
                "non_sensitive",
                "education",
                "healthcare",
                "worship",
                "public_safety",
                "justice_governance",
                "custodial_security",
                "military_security",
            ],
            "sensitive_subtype_coarse": [
                "non_sensitive",
                "social_sensitive",
                "security_sensitive",
            ] if args.emit_sensitive_coarse else None,
        },
    }
    (out_dir / "mlprep_metadata.json").write_text(json.dumps(metadata,
                                                             indent=2), encoding="utf-8")

    print(f"Wrote ML dataset:  {ml_parquet}")
    if args.write_full:
        print(f"Wrote labeled full: {full_parquet}")
    if do_crosstabs:
        print(f"Wrote crosstabs in: {out_dir / 'crosstabs'}")
    print(f"Wrote summaries in: {out_dir}")


if __name__ == "__main__":
    main()
