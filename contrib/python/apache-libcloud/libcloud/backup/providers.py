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

from libcloud.backup.types import Provider
from libcloud.common.providers import get_driver as _get_provider_driver
from libcloud.common.providers import set_driver as _set_provider_driver

DRIVERS = {
    Provider.DUMMY: ("libcloud.backup.drivers.dummy", "DummyBackupDriver"),
    Provider.EBS: ("libcloud.backup.drivers.ebs", "EBSBackupDriver"),
    Provider.GCE: ("libcloud.backup.drivers.gce", "GCEBackupDriver"),
    Provider.DIMENSIONDATA: (
        "libcloud.backup.drivers.dimensiondata",
        "DimensionDataBackupDriver",
    ),
}


def get_driver(provider):
    return _get_provider_driver(drivers=DRIVERS, provider=provider)


def set_driver(provider, module, klass):
    return _set_provider_driver(drivers=DRIVERS, provider=provider, module=module, klass=klass)
