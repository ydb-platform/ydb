#!/usr/bin/env python
# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#

import json
import os

DOT_ECMWF_OPENDATA = os.path.expanduser("~/.ecmwf-opendata")

URLS = {
    "ecmwf": "https://data.ecmwf.int/forecasts",
    "azure": "https://ai4edataeuwest.blob.core.windows.net/ecmwf",
    "aws": "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com",
    "ecmwf-esuites": "https://xdiss.ecmwf.int/ecpds/home/opendata",
    "google": "https://storage.googleapis.com/ecmwf-open-data",
}

if os.path.exists(DOT_ECMWF_OPENDATA):
    with open(DOT_ECMWF_OPENDATA) as f:
        URLS.update(json.load(f))

if "ECMWF_OPENDATA_URLS" in os.environ:
    URLS.update(json.loads(os.environ["ECMWF_OPENDATA_URLS"]))
