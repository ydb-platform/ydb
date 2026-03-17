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

from libcloud.storage.drivers.s3 import (
    S3_CDN_URL_EXPIRY_HOURS,
    S3StorageDriver,
    BaseS3StorageDriver,
    S3SignatureV4Connection,
)

__all__ = ["OvhStorageDriver"]

OVH_FR_SBG_HOST = "s3.sbg.perf.cloud.ovh.net"
OVH_FR_GRA_HOST = "s3.gra.perf.cloud.ovh.net"

# Maps OVH region name to connection hostname
REGION_TO_HOST_MAP = {
    "sbg": OVH_FR_SBG_HOST,
    "gra": OVH_FR_GRA_HOST,
}

NO_CDN_SUPPORT_ERROR = "CDN feature not implemented"


class OvhStorageDriver(BaseS3StorageDriver):
    name = "Ovh Storage Driver"
    website = "https://www.ovhcloud.com/en/public-cloud/object-storage/"
    connectionCls = S3SignatureV4Connection
    region_name = "sbg"

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        region="sbg",
        url=None,
        **kwargs,
    ):
        # Here for backward compatibility for old and deprecated driver class
        # per region approach
        if hasattr(self, "region_name") and not region:
            region = self.region_name  # pylint: disable=no-member

        self.region_name = region

        if region and region not in REGION_TO_HOST_MAP.keys():
            raise ValueError("Invalid or unsupported region: %s" % (region))

        self.name = "Ovh Object Storage (%s)" % (region)

        if host is None:
            self.connectionCls.host = REGION_TO_HOST_MAP[region]
        else:
            self.connectionCls.host = host

        super().__init__(
            key=key,
            secret=secret,
            secure=secure,
            host=host,
            port=port,
            region=region,
            url=url,
            **kwargs,
        )

    @classmethod
    def list_regions(self):
        return REGION_TO_HOST_MAP.keys()

    def get_object_cdn_url(self, obj, ex_expiry=S3_CDN_URL_EXPIRY_HOURS):
        # In order to download (private) objects we need to be able to generate a valid CDN URL,
        # hence shamefully just use the working code from the S3StorageDriver.
        return S3StorageDriver.get_object_cdn_url(self, obj, ex_expiry=ex_expiry)
