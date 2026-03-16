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

from libcloud.common.aws import SignedAWSConnection
from libcloud.common.types import LibcloudError
from libcloud.storage.drivers.s3 import S3Connection, BaseS3Connection, BaseS3StorageDriver

__all__ = ["DigitalOceanSpacesStorageDriver"]

DO_SPACES_HOSTS_BY_REGION = {
    "nyc3": "nyc3.digitaloceanspaces.com",
    "ams3": "ams3.digitaloceanspaces.com",
    "sfo2": "sfo2.digitaloceanspaces.com",
    "sgp1": "sgp1.digitaloceanspaces.com",
}
DO_SPACES_DEFAULT_REGION = "nyc3"
DEFAULT_SIGNATURE_VERSION = "2"
S3_API_VERSION = "2006-03-01"


class DOSpacesConnectionAWS4(SignedAWSConnection, BaseS3Connection):
    service_name = "s3"
    version = S3_API_VERSION

    def __init__(
        self,
        user_id,
        key,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        token=None,
        retry_delay=None,
        backoff=None,
        **kwargs,
    ):
        super().__init__(
            user_id,
            key,
            secure,
            host,
            port,
            url,
            timeout,
            proxy_url,
            token,
            retry_delay,
            backoff,
            signature_version=4,
        )


class DOSpacesConnectionAWS2(S3Connection):
    def __init__(
        self,
        user_id,
        key,
        secure=True,
        host=None,
        port=None,
        url=None,
        timeout=None,
        proxy_url=None,
        token=None,
        retry_delay=None,
        backoff=None,
        **kwargs,
    ):
        super().__init__(
            user_id,
            key,
            secure,
            host,
            port,
            url,
            timeout,
            proxy_url,
            token,
            retry_delay,
            backoff,
        )


class DigitalOceanSpacesStorageDriver(BaseS3StorageDriver):
    name = "DigitalOcean Spaces"
    website = "https://www.digitalocean.com/products/object-storage/"
    supports_chunked_encoding = False
    supports_s3_multipart_upload = True

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=DO_SPACES_DEFAULT_REGION,
        **kwargs,
    ):
        if region not in DO_SPACES_HOSTS_BY_REGION:
            raise LibcloudError("Unknown region (%s)" % (region), driver=self)

        host = DO_SPACES_HOSTS_BY_REGION[region]
        self.name = "DigitalOcean Spaces (%s)" % (region)

        self.region_name = region
        self.signature_version = str(kwargs.pop("signature_version", DEFAULT_SIGNATURE_VERSION))

        if self.signature_version == "2":
            self.connectionCls = DOSpacesConnectionAWS2
        elif self.signature_version == "4":
            self.connectionCls = DOSpacesConnectionAWS4
        else:
            raise ValueError("Invalid signature_version: %s" % (self.signature_version))
        self.connectionCls.host = host

        super().__init__(key, secret, secure, host, port, api_version, region, **kwargs)

    def _ex_connection_class_kwargs(self):
        kwargs = {}
        kwargs["signature_version"] = self.signature_version
        return kwargs
