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

from typing import Dict

from libcloud.dns.providers import Provider as DnsProvider
from libcloud.dns.providers import get_driver as get_dns_driver
from libcloud.backup.providers import Provider as BackupProvider
from libcloud.backup.providers import get_driver as get_backup_driver
from libcloud.compute.providers import Provider as ComputeProvider
from libcloud.compute.providers import get_driver as get_compute_driver
from libcloud.storage.providers import Provider as StorageProvider
from libcloud.storage.providers import get_driver as get_storage_driver
from libcloud.container.providers import Provider as ContainerProvider
from libcloud.container.providers import get_driver as get_container_driver
from libcloud.loadbalancer.providers import Provider as LoadBalancerProvider
from libcloud.loadbalancer.providers import get_driver as get_loadbalancer_driver


class DriverType:
    """Backup-as-a-service driver"""

    BACKUP = BackupProvider

    """ Compute-as-a-Service driver """
    COMPUTE = ComputeProvider

    """ Container-as-a-Service driver """
    CONTAINER = ContainerProvider

    """ DNS service provider driver """
    DNS = DnsProvider

    """ Load balancer provider-driver """
    LOADBALANCER = LoadBalancerProvider

    """ Storage-as-a-Service driver """
    STORAGE = StorageProvider


DriverTypeFactoryMap = {
    DriverType.BACKUP: get_backup_driver,
    DriverType.COMPUTE: get_compute_driver,
    DriverType.CONTAINER: get_container_driver,
    DriverType.DNS: get_dns_driver,
    DriverType.LOADBALANCER: get_loadbalancer_driver,
    DriverType.STORAGE: get_storage_driver,
}  # type: Dict


class DriverTypeNotFoundError(KeyError):
    def __init__(self, type):
        self.message = "Driver type '%s' not found." % type

    def __repr__(self):
        return self.message


def get_driver(type, provider):
    """
    Get a driver
    """
    try:
        return DriverTypeFactoryMap[type](provider)
    except KeyError:
        raise DriverTypeNotFoundError(type)
