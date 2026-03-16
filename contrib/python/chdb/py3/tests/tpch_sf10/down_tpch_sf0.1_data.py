#!/usr/bin/env python3

import os
import gdown

current_dir = os.path.dirname(os.path.abspath(__file__))

# Set the path to the TPCH data directory
small_data_dir = os.path.join(current_dir, "../data/tpchsf-small/")

# Check if data_dir exists
if not os.path.exists(small_data_dir):
    os.makedirs(small_data_dir)

# Download the data files
# gdown --folder https://drive.google.com/drive/folders/1mZC3NuPBZC4mjP3_kH18c9fLrv8ME7RU
gdown.download_folder(
    url="https://drive.google.com/drive/folders/16Lf5nAk8SCQoUiwOBY6LRxjhtB5egxQQ",
    output=os.path.dirname(small_data_dir),
    verify=True,
)
