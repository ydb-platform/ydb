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

from libcloud.common.aws import DEFAULT_SIGNATURE_VERSION, SignedAWSConnection
from libcloud.common.types import LibcloudError
from libcloud.storage.drivers.s3 import (
    API_VERSION,
    S3Connection,
    BaseS3Connection,
    BaseS3StorageDriver,
)

__all__ = ["S3RGWStorageDriver", "S3RGWOutscaleStorageDriver"]

S3_RGW_DEFAULT_REGION = "default"

S3_RGW_OUTSCALE_HOSTS_BY_REGION = {
    "eu-west-1": "osu.eu-west-1.outscale.com",
    "eu-west-2": "osu.eu-west-2.outscale.com",
    "us-west-1": "osu.us-west-1.outscale.com",
    "us-east-2": "osu.us-east-2.outscale.com",
    "cn-southeast-1": "osu.cn-southeast-1.outscale.hk",
}

S3_RGW_OUTSCALE_DEFAULT_REGION = "eu-west-2"


class S3RGWConnectionAWS4(SignedAWSConnection, BaseS3Connection):
    service_name = "s3"
    version = API_VERSION

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
            4,
        )  # force aws4


class S3RGWConnectionAWS2(S3Connection):
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


class S3RGWStorageDriver(BaseS3StorageDriver):
    name = "Ceph RGW"
    website = "http://ceph.com/"

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=S3_RGW_DEFAULT_REGION,
        **kwargs,
    ):
        if host is None:
            raise LibcloudError("host required", driver=self)

        self.name = kwargs.pop("name", None)
        if self.name is None:
            self.name = "Ceph RGW S3 (%s)" % (region)

        self.ex_location_name = region
        self.region_name = region
        self.signature_version = str(kwargs.pop("signature_version", DEFAULT_SIGNATURE_VERSION))

        if self.signature_version not in ["2", "4"]:
            raise ValueError("Invalid signature_version: %s" % (self.signature_version))

        if self.signature_version == "2":
            self.connectionCls = S3RGWConnectionAWS2
        elif self.signature_version == "4":
            self.connectionCls = S3RGWConnectionAWS4
        self.connectionCls.host = host

        super().__init__(key, secret, secure, host, port, api_version, region, **kwargs)

    def _ex_connection_class_kwargs(self):
        kwargs = {}
        kwargs["signature_version"] = self.signature_version
        return kwargs


class S3RGWOutscaleStorageDriver(S3RGWStorageDriver):
    name = "RGW Outscale"
    website = "https://en.outscale.com/"

    def __init__(
        self,
        key,
        secret=None,
        secure=True,
        host=None,
        port=None,
        api_version=None,
        region=S3_RGW_OUTSCALE_DEFAULT_REGION,
        **kwargs,
    ):
        if region not in S3_RGW_OUTSCALE_HOSTS_BY_REGION:
            raise LibcloudError("Unknown region (%s)" % (region), driver=self)
        host = S3_RGW_OUTSCALE_HOSTS_BY_REGION[region]
        kwargs["name"] = "OUTSCALE Ceph RGW S3 (%s)" % region
        super().__init__(key, secret, secure, host, port, api_version, region, **kwargs)
