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

from libcloud.common.types import LibcloudError
from libcloud.storage.providers import Provider
from libcloud.storage.drivers.s3 import BaseS3Connection, BaseS3StorageDriver

__all__ = ["AuroraObjectsStorageDriver"]

AURORA_OBJECTS_EU_HOST = "o.auroraobjects.eu"

NO_CDN_SUPPORT_ERROR = "CDN is not supported by AuroraObjects"


class BaseAuroraObjectsConnection(BaseS3Connection):
    host = AURORA_OBJECTS_EU_HOST


class BaseAuroraObjectsStorageDriver(BaseS3StorageDriver):
    type = Provider.AURORAOBJECTS
    name = "PCextreme AuroraObjects"
    website = "https://www.pcextreme.com/aurora/objects"


class AuroraObjectsStorageDriver(BaseAuroraObjectsStorageDriver):
    connectionCls = BaseAuroraObjectsConnection

    def enable_container_cdn(self, *argv):
        raise LibcloudError(NO_CDN_SUPPORT_ERROR, driver=self)

    def enable_object_cdn(self, *argv):
        raise LibcloudError(NO_CDN_SUPPORT_ERROR, driver=self)

    def get_container_cdn_url(self, *argv):
        raise LibcloudError(NO_CDN_SUPPORT_ERROR, driver=self)

    def get_object_cdn_url(self, *argv):
        raise LibcloudError(NO_CDN_SUPPORT_ERROR, driver=self)
