# From Connectivity to Context: Discovering Correlations between Land Use and Network Infrastructure Data

## Abstract

Wi-Fi access points (APs) periodically transmit 802.11 beacon frames to advertise their presence. These frames contain the unique layer 2 identifier of the device, known as the Basic Service Set Identifier (BSSID). We hypothesize that when BSSID observations are aggregated at scale and correlated with openly available geolocation data, they can support inferences about the physical context surrounding Wi-Fi APs. The ability to infer real-world characteristics without direct physical access can reveal key privacy risks for everyday environments and sensitive infrastructure. In this paper, we explore the feasibility of a privacy attack by building a data pipeline for ingestion, pre-processing, and feature engineering of large-scale Wi-Fi BSSID telemetry datasets, IEEE OUIs, and OpenStreetMap geolocation data. We analyze multiple land-use inference models, designing and evaluating two experiments that explore (1) multiclass land-use prediction and sensitive-context binary prediction and (2) predictions from an ensemble model using density-based geospatial clustering. We find that extracting meaningful context from our inference models is possible but automation and performance is constrained by the large variance in OpenStreetMap tags and limited available features. Nonetheless, this exposure of sensitive land infrastructure types can enhance threat actors’ open-source reconnaissance, enabling them to aggregate and correlate OSINT datasets to map critical assets, infer operational patterns, and identify potential targets.

---

# Repository Structure

- `data_preprocessing/`: Scripts for vendor enrichment, reverse geolocation, OSM feature processing, and ML label generation.
- `Exp1a-Notebooks/`: Notebooks for Experiment 1a, covering land-use classification workflows.
- `Exp1b-Notebooks/`: Notebooks for Experiment 1b, covering sensitive-context classification workflows.
- `exp1a-results/`: Output files and evaluation summaries from Experiment 1a.
- `exp1b-results/`: Output files and evaluation summaries from Experiment 1b.
- `Exp2-EnsembleModel/`: HTML display files generated for Experiment 2, focused on clustering and ensemble-based geospatial inference.

---

## Data Preprocessing

This directory contains the core scripts responsible for cleaning, enriching, and preparing raw datasets for analysis and machine learning workflows.

### Directory Structure

```text
data_preprocessing/
├── data_enrichment.py       # Vendor identification (OUI) and reverse geolocation
├── distribution_analysis.py # Statistical distribution and crosstab analysis
├── ml_preprocess.py         # Feature engineering and label bucketing for ML
├── vendor_normalization.py  # Standardization of vendor naming conventions
├── oui_cleaner.py       # Parser for IEEE OUI registry files
├── oui_Feb_3_2026.txt   # Raw IEEE OUI registry (Retrieved 2026-02-03)
└── ouiclean.txt         # Standardized vendor reference file
```

---

### Data Enrichment (`data_enrichment.py`)
Associates raw data entries with context by performing two lookups:
*   **Vendor Identification:** Matches MAC address Organizational Unique Identifiers (OUI) to known device manufacturers, with integrated bit-flipping. (Section 4.1.2)
*   **Reverse  Geolocation:** Interfaces with our Nominatim server to extract reverse geolocation information from OpenStreetMaps. (Section 4.1.3)

#### OUI Cleaner (`oui_cleaner.py`)
A utility script designed to parse the raw text format provided by the IEEE. It extracts the relevant Organizationally Unique Identifiers and their associated vendor names, transforming the data from `oui_Feb_3_2026.txt` into a structured `ouiclean.txt` format. (Section 4.1.2)

#### Raw OUI Registry (`oui_Feb_3_2026.txt`)
The original, unformatted OUI dataset as accessed from the IEEE on **February 3, 2026**. This file serves as the ground-truth source for all device manufacturer lookups within the repository. (Section 4.1.2)

#### Processed OUI List (`ouiclean.txt`)
The optimized and cleaned version of the registry. This file is the primary reference used by the `data_enrichment.py` script to perform efficient vendor matching for the project's data entries. (Section 4.1.2)

### Distribution Analysis (`distribution_analysis.py`)
A diagnostic tool used to explore raw data characteristics. It generates distributions for vendors and location labels, as well as crosstabulations to identify patterns and inform downstream preprocessing decisions. (Section 4.2)

### Vendor Normalization (`vendor_normalization.py`)
Handles the cleaning of vendor metadata. This script maps various OUI registration aliases and naming inconsistencies (e.g., "Company Inc." vs "Company Ltd.") to a single, unified canonical name. (Section 4.3.2)

### ML Preprocessing (`ml_preprocess.py`)
Label bucketing for machine learning. This script categorizes OSM-derived features into labeled buckets based on feature semantics. It assigns hierarchical labels to each data row: (1) `label_target_main` for land-use context; and (2) `label_sensitive_binary` and `label_sensitive_subtype` for sensitive context inference. (Section 4.3.1)

---

## Experiment 1 Notebooks

The Experiment 1 notebooks document the supervised machine learning workflows used to evaluate whether Wi-Fi infrastructure metadata can support land-use and sensitive-context inference. They include data loading, feature prep verification, train/test splitting, model training, evaluation, and result inspection.

### `Exp1a-Notebooks/` and `Exp1b-Notebooks/`

These directories contain notebooks for the two Experiment 1 tasks. `Exp1a-Notebooks/` focuses on multiclass land-use classification, while `Exp1b-Notebooks/` focuses on sensitive-context binary classification. The notebooks compare model behavior under both random splits and state-based splits to evaluate whether learned patterns generalize across geographic regions.

`ml_modeling_baseline.ipynb` serves as a baseline template for test runs and helps keep the Experiment 1 modeling workflow consistent across models. The final notebook used for Experiment 1a is `ml_modeling_03.ipynb`. The final notebook used for Experiment 1b is `ml_modeling_01.ipynb`. `ml_modeling_02.ipynb` is used as a feature-engineering playground for diagnostics and testing in Experiment 1b.

---

## Experiment Results

### Experiment 1

The Experiment 1 results directories store generated outputs from the modeling notebooks, including model performance tables, evaluation summaries, and supporting artifacts used in the paper. These files make the experiment outputs easier to inspect without rerunning the full pipeline.

`exp1a-results/` contains saved outputs from the multiclass land-use classification task.

`exp1b-results/` contains saved outputs from the sensitive-context binary classification task.

### Experiment 2

`Exp2-EnsembleModel/` contains HTML display files for Experiment 2.
