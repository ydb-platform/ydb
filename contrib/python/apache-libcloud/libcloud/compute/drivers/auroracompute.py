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

from libcloud.compute.providers import Provider
from libcloud.compute.drivers.cloudstack import CloudStackNodeDriver

__all__ = ["AuroraComputeRegion", "AuroraComputeNodeDriver"]


class AuroraComputeRegion:
    AMS = "Amsterdam"
    RTD = "Rotterdam"
    MIA = "Miami"
    LAX = "Los Angeles"
    TYO = "Tokyo"
    BCN = "Barcelona"


REGION_ENDPOINT_MAP = {
    AuroraComputeRegion.AMS: "/ams",
    AuroraComputeRegion.RTD: "/rtd",
    AuroraComputeRegion.MIA: "/mia",
    AuroraComputeRegion.LAX: "/lax",
    AuroraComputeRegion.TYO: "/tyo",
    AuroraComputeRegion.BCN: "/bcn",
}


class AuroraComputeNodeDriver(CloudStackNodeDriver):
    type = Provider.AURORACOMPUTE
    name = "PCextreme AuroraCompute"
    website = "https://www.pcextreme.com/aurora/compute"

    def __init__(self, key, secret, path=None, host=None, url=None, region=None):
        if host is None:
            host = "api.auroracompute.eu"

        if path is None:
            path = REGION_ENDPOINT_MAP.get(region, "/ams")

        super().__init__(key=key, secret=secret, host=host, path=path, secure=True)
