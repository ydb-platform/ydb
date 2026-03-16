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

__all__ = ["ScalewayStorageDriver"]

SCW_FR_PAR_STANDARD_HOST = "s3.fr-par.scw.cloud"
SCW_NL_AMS_HOST = "s3.nl-ams.scw.cloud"
SCW_PL_WAR_HOST = "s3.pl-waw.scw.cloud"

# Maps SCW region name to connection hostname
REGION_TO_HOST_MAP = {
    "fr-par": SCW_FR_PAR_STANDARD_HOST,
    "nl-ams": SCW_NL_AMS_HOST,
    "pl-waw": SCW_PL_WAR_HOST,
}

NO_CDN_SUPPORT_ERROR = "CDN feature not implemented"


class ScalewayStorageDriver(BaseS3StorageDriver):
    name = "Scaleway Storage Driver"
    website = "https://www.scaleway.com/en/object-storage/"
    connectionCls = S3SignatureV4Connection
    region_name = "fr-par"

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        region="fr-par",
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

        self.name = "Scaleway Object Storage (%s)" % (region)

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
