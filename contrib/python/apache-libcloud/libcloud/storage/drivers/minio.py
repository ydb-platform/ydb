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
from libcloud.storage.drivers.s3 import API_VERSION, BaseS3Connection, BaseS3StorageDriver

__all__ = ["MinIOStorageDriver"]


class MinIOConnectionAWS4(SignedAWSConnection, BaseS3Connection):
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


class MinIOStorageDriver(BaseS3StorageDriver):
    name = "MinIO Storage Driver"
    website = "https://min.io/"
    connectionCls = MinIOConnectionAWS4
    region_name = ""

    def __init__(self, key, secret=None, secure=True, host=None, port=None):
        if host is None:
            raise LibcloudError("host argument is required", driver=self)

        self.connectionCls.host = host

        super().__init__(key=key, secret=secret, secure=secure, host=host, port=port)
