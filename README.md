# Abstract

\WiFi access points (APs) periodically transmit 802.11 beacon frames to advertise their presence. These frames contain the unique layer 2 identifier of the device, known as the Basic Service Set Identifier (BSSID). We hypothesize that when BSSID observations are aggregated at scale and correlated with openly available geolocation data, they can support inferences about the physical context surrounding Wi-Fi APs. The ability to infer real-world characteristics without direct physical access can reveal key privacy risks for everyday environments and sensitive infrastructure. In this paper, we explore the feasibility of a privacy attack by building a data pipeline for ingestion, pre-processing, and feature engineering of large-scale Wi-Fi BSSID telemetry datasets, IEEE OUIs, and OpenStreetMap geolocation data. We analyze multiple land-use inference models, designing and evaluating two experiments that explore (1) multiclass land-use prediction and sensitive-context binary prediction and (2) predictions from an ensemble model using density-based geospatial clustering. We find that extracting meaningful context from our inference models is possible but automation and performance is constrained by the large variance in OpenStreetMap tags and limited available features. Nonetheless, this exposure of sensitive land infrastructure types can enhance threat actors’ open-source reconnaissance, enabling them to aggregate and correlate OSINT datasets to map critical assets, infer operational patterns, and identify potential targets.

---

# Data Preprocessing

This directory contains the core scripts responsible for cleaning, enriching, and preparing raw datasets for analysis and machine learning workflows.

## Directory Structure

```text
data_preprocessing/
├── data_enrichment.py       # Vendor identification (OUI) and reverse geolocation
├── distribution_analysis.py # Statistical distribution and crosstab analysis
├── ml_preprocess.py         # Feature engineering and label bucketing for ML
└── vendor_normalization.py  # Standardization of vendor naming conventions
```

---

## File Descriptions

### Data Enrichment (`data_enrichment.py`)
Associates raw data entries with context by performing two lookups:
*   **Vendor Identification:** Matches MAC address Organizational Unique Identifiers (OUI) to known device manufacturers.
*   **Reverse  Geolocation:** Interfaces with our Nominatim server to extract reverse geolocation information from OpenStreetMaps.

### Distribution Analysis (`distribution_analysis.py`)
A diagnostic tool used to explore raw data characteristics. It generates distributions for vendors and location labels, as well as crosstabulations to identify patterns and inform downstream preprocessing decisions.

### Vendor Normalization (`vendor_normalization.py`)
Handles the cleaning of vendor metadata. This script maps various OUI registration aliases and naming inconsistencies (e.g., "Company Inc." vs "Company Ltd.") to a single, unified canonical name.

### ML Preprocessing (`ml_preprocess.py`)
Final-stage preparation for machine learning. It transforms OSM features and performs label bucketing, categorizing OSM subtypes into **Sensitive** and **Non-Sensitive** labels and respective buckets to enable further classification tasks.
