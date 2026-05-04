"""Vendor normalization lookup and helpers.

This file is intentionally simple: keep vendor mapping here, and let other scripts
load it as a single source of truth.
"""

from __future__ import annotations
from typing import Dict


VENDOR_MAP = {
    "Hewlett Packard": [
        "Hewlett Packard Enterprise", "HP Inc.", "Hewlett Packard",
        "ProCurve Networking by HP"
    ],
    "Cisco": [
        "Cisco Systems, Inc", "Cisco Meraki", "Cisco-Linksys, LLC",
        "Cisco SPVTG", "Cisco Systems"
    ],
    "Vantiva": [
        "Vantiva USA LLC", "Vantiva - Connected Home", "Vantiva Connected Home - Home Networks",
        "Vantiva Technologies Belgium", "Vantiva Connected Home - Technologies Telco"
    ],
    "Linksys": [
        "The Linksys Group, Inc.", "Linksys USA, Inc"
    ],
    "Quanta Computer Inc.": [
        "Quanta Computer Inc.", "Quanta Microsystems,Inc.", "Quanta Microsystems, INC."
    ],
    "TP-Link": [
        "TP-Link Systems Inc", "TP-LINK TECHNOLOGIES CO.,LTD.",
        "TP-Link Systems Inc."
    ],
    "Samsung": [
        "Samsung Electronics Co.,Ltd", "Samsung Electronics",
        "SAMSUNG ELECTRO-MECHANICS(THAILAND)", "Samsung Thales",
        "SAMSUNG ELECTRO MECHANICS CO., LTD.", "Samsung Electronics Co., Ltd."
    ],
    "Broadcom": [
        "Broadcom", "Broadcom Limited", "Broadcom Technologies"
    ],
    "Nokia": [
        "Nokia Solutions and Networks GmbH & Co. KG", "Nokia Siemens Networks GmbH & Co. KG.", "Nokia Siemens Networks GmbH & Co. KG"
        , "Nokia Solutions and Networks India Private Limited", "Nokia Danmark A/S", "NOKIA WIRELESS BUSINESS COMMUN",
        "Nokia Danmark A/S", "Nokia Shanghai Bell Co., Ltd.", "Nokia Corporation", "Nokia", "Nokia Solutions and Networks India Private Limited"
    ],
    "General Electric": [
        "General Electric Consumer and Industrial", "GE Appliances", "GE Lighting", "GENERAL ELECTRIC CORPORATION", "GE Security",
        "GE Transportation Systems"
    ],
    "Hisense": [
        "Hisense Electric Co.,Ltd", "Qingdao Hisense Communications Co.,Ltd.", "Hisense broadband multimedia technology Co.,Ltd",
        "Qingdao Hisense Electronics Co.,Ltd.", "HISENSE ELECTRIC CO.,LTD"
    ],
    "Xiaomi": [
        "Beijing Xiaomi Mobile Software Co., Ltd", "Xiaomi Communications Co Ltd", "Beijing Xiaomi Electronics Co.,Ltd",
        "Beijing Xiaomi Electronics Co., Ltd.", "Xiaomi Communications Co Ltd", "XIAOMI Electronics,CO.,LTD"
    ],
    "WiZ Home Monitoring": [
        "WiZ", "WiZ Connected Lighting Company Limited", "WiZ IoT Company Limited", "Wiznet"
    ],
    "Nvidia Inc.": [
        "NVIDIA", "Nvidia Inc."
    ],
    "Google": [
        "Google, Inc.", "Google", "Google LLC"
    ],
    "Motorola": [
        "Motorola Solutions Inc.", "Motorola Mobility LLC, a Lenovo Company", "Motorola - BSG", "Motorola, Broadband Solutions Group", "Motorola (Wuhan) Mobility Technologies Communication Co., Ltd.", "MOTOROLA"
    ],
    "Flextronics": [
        "Flextronics International", "Flextronics Computing(Suzhou)Co.,Ltd.", "FLEXTRONICS TECHNOLOGIES MEXICO S DE RL DE CV", "FLEXTRONICS"
    ],
    "Hitachi Energy USA Inc.": [
        "Hitachi Energy USA Inc.", "Hitachi Denshi, Ltd."
    ],
    "YAMAHA CORPORATION": [
        "YAMAHA CORPORATION", "YAMAHA MOTOR CO.,LTD"
    ],
    # "Verizon": [
    #   "Verizon", "Verizon Business"
    # ],
    "Amazon": [
        "Amazon Technologies Inc.", "Blink by Amazon", "Amazon.com, LLC", "Amazon.com"
    ],
    "Vivint": [
        "Vivint Smart Home", "Vivint Inc", "Vivint Wireless Inc."
    ],
    "Nintendo": [
        "Nintendo Co.,Ltd", "Nintendo Co., Ltd."
    ],
    "Raspberry Pi": [
        "Raspberry Pi Trading Ltd", "Raspberry Pi Foundation", "Raspberry Pi (Trading) Ltd"
    ],
    "D-Link": [
        "D-Link International", "D-Link Corporation", "D-Link (Shanghai) Limited Corp.", "D-LINK SYSTEMS, INC."
    ],
    "Hon Hai (Foxconn)": [
        "Hon Hai Precision Ind. Co.,Ltd.", "Hon Hai Precision Industry Co., Ltd.", "Hon Hai Precision IND.CO.,LTD", "Hon Hai Precision Industry Co.,LTD",

    ],
    "Peplink": [
        "Peplink International Ltd.", "Pepwave Limited", "PePWave Ltd"
    ],
    "Grandstream": [
        "Grandstream Networks, Inc.", "Grandstream Networks Inc"
    ],
    "Actiontec": [
        "Actiontec Electronics, Inc", "Actiontec Electronics Inc."
    ],
    "Arcadyan": [
        "Arcadyan Corporation", "Arcadyan Technology Corporation"
    ],
    "Roku": [
        "Roku, Inc", "Roku, Inc."
    ],
    "Apple": [
        "Apple, Inc.", "Apple Inc."
    ],
    "BYD": [
        "Huizhou BYD Electronic Co., Ltd.", "BYD Precision Manufacture Co.,Ltd", "BYD Precision Manufacture Company Ltd."
    ],
    "SonicWall": [
        "SonicWall", "Sonicwall", "SonicWALL"
    ],
    "Liteon": [
        "Liteon Technology Corporation", "LITE-ON IT Corporation", "LITE-ON Technology Corp.", "LITE-ON Communications, Inc."
    ],
    "Belkin": [
        "Belkin International Inc.", "BELKIN COMPONENTS", "Belkin Corporation"
    ],
    "Humax": [
        "HUMAX Co., Ltd.", "HUMAX NETWORKS", "HUMAX Co., Ltd."
    ],
    "Sercomm": [
        "SERCOMM PHILIPPINES INC", "Sercomm Corporation.", "Sercomm Japan Corporation"
    ],
    "SJIT": [
        "SJI Industry Company", "SJIT", "SJIT Co., Ltd."
    ],
    "AMPAK": [
        "AMPAK Technology,Inc.", "AMPAK Technology, Inc."
    ],
    "Microchip": [
        "Microchip Technology Inc.", "Microchip Technologies Inc"
    ],
    "AltoBeam": [
        "AltoBeam Inc.", "AltoBeam (China) Inc.", "AltoBeam (Xiamen) Technology Ltd, Co."
    ],
    "SkyBell": [
        "SkyBell Technologies Inc.", "SKYBELL, INC"
    ],
    "Kyocera": [
        "KYOCERA Display Corporation", "KYOCERA Document Solutions Inc.", "KYOCERA CORPORATION", "Kyocera Wireless Corp.", "KYOCERA Corporation",
        "KYOCERA Document Solutions Inc."
    ],
    "Wistron": [
        "Wistron Infocomm (Zhongshan) Corporation", "Wistron InfoComm(Kunshan)Co.,Ltd.", "Wistron Corporation", "Wistron Mexico SA de CV"
    ],
    "PT Inovacao": [
        "PT Inovação e Sistemas SA", "PT Inovacao"
    ],
    "Tesla": [
        "Tesla,Inc.", "Tesla Inc"
    ],
    "InHand": [
        "Beijing InHand Networking Technology Co.,Ltd.", "InHand Networks, INC."
    ],
    "MXCHIP": [
        "Shanghai MXCHIP Information Technology Co., Ltd.", "MXCHIP Company Limited"
    ],
    "Billion Electric": [
        "Billion Electric Co. Ltd.", "Billion Electric Co., Ltd."
    ],
    "Beijer Electronics": [
        "Beijer Electronics Corp.", "Beijer Electronics Products AB"
    ],
    "Trolink": [
        "Shenzhen Trolink Technology CO, LTD", "shenzhen trolink Technology Co.,Ltd", "Shenzhen Trolink Technology Co.,LTD"
    ],
    "Teltonika": [
        "Teltonika", "TELTONIKA NETWORKS UAB"
    ],
    "Fujitsu": [
        "FUJITSU LIMITED", "Fujitsu Softek", "Fujitsu Siemens Computers"
    ],
    "Sichuan Tianyi": [
        "Sichuan Tianyi Comheart Telecom Co.,LTD",
        "Sichuan Tianyi Information Science & Technology Stock CO.,LTD",
        "Sichuan tianyi kanghe communications co., LTD"
    ],
    "ASUS": [
        "ASUSTek COMPUTER INC.", "ASUS", "Asustek", "Asus Network Technologies, Inc."
    ],
    "Sonos": [
        "Sonos, Inc.", "Sonos Inc."
    ],
    "Intel": [
        "Intel Corporate", "Intel Corporation", "Intel"
    ],
    "Guangzhou Juan": [
        "Guangzhou Juan Optical and Electronical Tech Joint Stock Co., Ltd",
        "Guangzhou Juan Intelligent Tech Joint Stock Co.,Ltd",
        "Guangdong Juan Intelligent Technology Joint Stock Co., Ltd."
    ],
    "Edgecore": [
        "Edgecore Networks Corporation", "Edgecore Americas Networking Corporation"
    ],
    "Unionman": [
        "UNION MAN TECHNOLOGY CO.,LTD", "UNIONMAN TECHNOLOGY CO.,LTD"
    ],
    "Micro-Star (MSI)": [
        "MICRO-STAR INTERNATIONAL CO., LTD.", "MICRO-STAR INT'L CO.,LTD", "MSI Technology GmbH"
    ],
    "Microsoft": [
        "Microsoft Corporation", "Microsoft", "MICROSOFT CORP.", "Microsoft Corp.", "Microsoft Mobile Oy"
    ]
}


def normalize_vendor_name(value: str) -> str:
    """Normalize vendor text for case/whitespace-insensitive matching."""
    if value is None:
        return ""
    return " ".join(str(value).strip().lower().split())


def build_vendor_alias_map(vendor_map: dict[str, list[str]] | None = None) -> Dict[str, str]:
    """Return normalized alias -> canonical vendor mapping."""
    src = vendor_map or VENDOR_MAP
    alias_map: Dict[str, str] = {}
    for canonical, variants in src.items():
        canonical_norm = normalize_vendor_name(canonical)
        if canonical_norm:
            alias_map[canonical_norm] = canonical
        for raw in variants:
            key = normalize_vendor_name(raw)
            if key:
                alias_map[key] = canonical
    return alias_map
