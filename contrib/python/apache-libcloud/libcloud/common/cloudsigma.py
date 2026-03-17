# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__all__ = [
    "API_ENDPOINTS_1_0",
    "API_ENDPOINTS_2_0",
    "API_VERSIONS",
    "INSTANCE_TYPES",
    "MAX_VIRTIO_CONTROLLERS",
    "MAX_VIRTIO_UNITS",
]

# API end-points
API_ENDPOINTS_1_0 = {
    "zrh": {
        "name": "Zurich",
        "country": "Switzerland",
        "host": "api.zrh.cloudsigma.com",
    },
    "lvs": {
        "name": "Las Vegas",
        "country": "United States",
        "host": "api.lvs.cloudsigma.com",
    },
}

API_ENDPOINTS_2_0 = {
    "zrh": {
        "name": "Zurich, Switzerland",
        "country": "Switzerland",
        "host": "zrh.cloudsigma.com",
    },
    "sjc": {
        "name": "San Jose, CA",
        "country": "United States",
        "host": "sjc.alpha3cloud.com",
    },
    "wdc": {
        "name": "Washington, DC",
        "country": "United States",
        "host": "wdc.alpha3cloud.com",
    },
    "hnl": {
        "name": "Honolulu, HI",
        "country": "United States",
        "host": "hnl.cloudsigma.com",
    },
    "per": {
        "name": "Perth, Australia",
        "country": "Australia",
        "host": "per.cloudsigma.com",
    },
    "mnl": {
        "name": "Manila, Philippines",
        "country": "Philippines",
        "host": "mnl.cloudsigma.com",
    },
    "fra": {
        "name": "Frankfurt, Germany",
        "country": "Germany",
        "host": "fra.cloudsigma.com",
    },
    "mel": {
        "name": "Melbourne, Australia",
        "country": "Australia",
        "host": "mel.cloudsigma.com",
    },
    "dbl": {
        "name": "Dublin, Ireland",
        "country": "Ireland",
        "host": "ec.servecentric.com",
    },
    "tyo": {"name": "Tokyo, Japan", "country": "Japan", "host": "tyo.cloudsigma.com"},
    "crk": {
        "name": "Clark, Philippines",
        "country": "Philippines",
        "host": "crk.cloudsigma.com",
    },
    "mnl2": {
        "name": "Manila-2, Philippines",
        "country": "Philippines",
        "host": "mnl2.cloudsigma.com",
    },
    "ruh": {
        "name": "Riyadh, Saudi Arabia",
        "country": "Saudi Arabia",
        "host": "ruh.cloudsigma.com",
    },
    "bdn": {"name": "Boden, Sweden", "country": "Sweden", "host": "cloud.hydro66.com"},
    "gva": {
        "name": "Geneva, Switzerland",
        "country": "Switzerland",
        "host": "gva.cloudsigma.com",
    },
}

DEFAULT_REGION = "zrh"

# Supported API versions.
API_VERSIONS = ["1.0" "2.0"]  # old and deprecated

DEFAULT_API_VERSION = "2.0"

# CloudSigma doesn't specify special instance types.
# Basically for CPU any value between 0.5 GHz and 20.0 GHz should work,
# 500 MB to 32000 MB for ram
# and 1 GB to 1024 GB for hard drive size.
# Plans in this file are based on examples listed on https://cloudsigma
# .com/pricing/
INSTANCE_TYPES = [
    {
        "id": "small-1",
        "name": "small-1, 1 CPUs, 512MB RAM, 50GB disk",
        "cpu": 1,
        "memory": 512,
        "disk": 50,
        "bandwidth": None,
    },
    {
        "id": "small-2",
        "name": "small-2, 1 CPUs, 1024MB RAM, 50GB disk",
        "cpu": 1,
        "memory": 1024,
        "disk": 50,
        "bandwidth": None,
    },
    {
        "id": "small-3",
        "name": "small-3, 1 CPUs, 2048MB RAM, 50GB disk",
        "cpu": 1,
        "memory": 2048,
        "disk": 50,
        "bandwidth": None,
    },
    {
        "id": "medium-1",
        "name": "medium-1, 2 CPUs, 2048MB RAM, 50GB disk",
        "cpu": 2,
        "memory": 2048,
        "disk": 50,
        "bandwidth": None,
    },
    {
        "id": "medium-2",
        "name": "medium-2, 2 CPUs, 4096MB RAM, 60GB disk",
        "cpu": 2,
        "memory": 4096,
        "disk": 60,
        "bandwidth": None,
    },
    {
        "id": "medium-3",
        "name": "medium-3, 4 CPUs, 8192MB RAM, 80GB disk",
        "cpu": 4,
        "memory": 8192,
        "disk": 80,
        "bandwidth": None,
    },
    {
        "id": "large-1",
        "name": "large-1, 8 CPUs, 16384MB RAM, 160GB disk",
        "cpu": 8,
        "memory": 16384,
        "disk": 160,
        "bandwidth": None,
    },
    {
        "id": "large-2",
        "name": "large-2, 12 CPUs, 32768MB RAM, 320GB disk",
        "cpu": 12,
        "memory": 32768,
        "disk": 320,
        "bandwidth": None,
    },
    {
        "id": "large-3",
        "name": "large-3, 16 CPUs, 49152MB RAM, 480GB disk",
        "cpu": 16,
        "memory": 49152,
        "disk": 480,
        "bandwidth": None,
    },
    {
        "id": "xlarge",
        "name": "xlarge, 20 CPUs, 65536MB RAM, 640GB disk",
        "cpu": 20,
        "memory": 65536,
        "disk": 640,
        "bandwidth": None,
    },
]

# mapping between cpus, ram, disk to example size attributes
SPECS_TO_SIZE = {
    (1, 512, 50): {
        "id": "small-1",
        "name": "small-1, 1 CPUs, 512MB RAM, 50GB disk",
        "cpu": 1,
        "ram": 512,
        "disk": 50,
        "bandwidth": None,
        "price": None,
    },
    (1, 1024, 50): {
        "id": "small-2",
        "name": "small-2, 1 CPUs, 1024MB RAM, 50GB disk",
        "cpu": 1,
        "ram": 1024,
        "disk": 50,
        "bandwidth": None,
        "price": None,
    },
    (1, 2048, 50): {
        "id": "small-3",
        "name": "small-3, 1 CPUs, 2048MB RAM, 50GB disk",
        "cpu": 1,
        "ram": 2048,
        "disk": 50,
        "bandwidth": None,
        "price": None,
    },
    (2, 2048, 50): {
        "id": "medium-1",
        "name": "medium-1, 2 CPUs, 2048MB RAM, 50GB disk",
        "cpu": 2,
        "ram": 2048,
        "disk": 50,
        "bandwidth": None,
        "price": None,
    },
    (2, 4096, 60): {
        "id": "medium-2",
        "name": "medium-2, 2 CPUs, 4096MB RAM, 60GB disk",
        "cpu": 2,
        "ram": 4096,
        "disk": 60,
        "bandwidth": None,
        "price": None,
    },
    (4, 8192, 80): {
        "id": "medium-3",
        "name": "medium-3, 4 CPUs, 8192MB RAM, 80GB disk",
        "cpu": 4,
        "ram": 8192,
        "disk": 80,
        "bandwidth": None,
        "price": None,
    },
    (8, 16384, 160): {
        "id": "large-1",
        "name": "large-1, 8 CPUs, 16384MB RAM, 160GB disk",
        "cpu": 8,
        "ram": 16384,
        "disk": 160,
        "bandwidth": None,
        "price": None,
    },
    (12, 32768, 320): {
        "id": "large-2",
        "name": "large-2, 12 CPUs, 32768MB RAM, 320GB disk",
        "cpu": 12,
        "ram": 32768,
        "disk": 320,
        "bandwidth": None,
        "price": None,
    },
    (16, 49152, 480): {
        "id": "large-3",
        "name": "large-3, 16 CPUs, 49152MB RAM, 480GB disk",
        "cpu": 16,
        "ram": 49152,
        "disk": 480,
        "bandwidth": None,
        "price": None,
    },
    (20, 65536, 640): {
        "id": "xlarge",
        "name": "xlarge, 20 CPUs, 65536MB RAM, 640GB disk",
        "cpu": 20,
        "ram": 65536,
        "disk": 640,
        "bandwidth": None,
        "price": None,
    },
}

MAX_VIRTIO_CONTROLLERS = 203
MAX_VIRTIO_UNITS = 4
