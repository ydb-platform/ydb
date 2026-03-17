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

from types import ModuleType
from typing import TYPE_CHECKING, Type, Union

from libcloud.container.types import Provider
from libcloud.common.providers import get_driver as _get_provider_driver
from libcloud.common.providers import set_driver as _set_provider_driver

if TYPE_CHECKING:
    # NOTE: This is needed to avoid having setup.py depend on requests
    from libcloud.container.base import ContainerDriver

DRIVERS = {
    Provider.DUMMY: ("libcloud.container.drivers.dummy", "DummyContainerDriver"),
    Provider.DOCKER: ("libcloud.container.drivers.docker", "DockerContainerDriver"),
    Provider.ECS: ("libcloud.container.drivers.ecs", "ElasticContainerDriver"),
    Provider.KUBERNETES: (
        "libcloud.container.drivers.kubernetes",
        "KubernetesContainerDriver",
    ),
    Provider.LXD: ("libcloud.container.drivers.lxd", "LXDContainerDriver"),
    Provider.RANCHER: ("libcloud.container.drivers.rancher", "RancherContainerDriver"),
    Provider.GKE: ("libcloud.container.drivers.gke", "GKEContainerDriver"),
}


def get_driver(provider):
    # type: (Union[Provider, str]) -> Type[ContainerDriver]
    return _get_provider_driver(drivers=DRIVERS, provider=provider)


def set_driver(provider, module, klass):
    # type: (Union[Provider, str], ModuleType, type) -> Type[ContainerDriver]
    return _set_provider_driver(drivers=DRIVERS, provider=provider, module=module, klass=klass)
