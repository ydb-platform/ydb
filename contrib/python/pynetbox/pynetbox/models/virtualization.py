"""
(c) 2017 DigitalOcean

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from pynetbox.core.endpoint import DetailEndpoint
from pynetbox.core.response import JsonField, Record
from pynetbox.models.ipam import IpAddresses


class VirtualMachines(Record):
    primary_ip = IpAddresses
    primary_ip4 = IpAddresses
    primary_ip6 = IpAddresses
    config_context = JsonField

    @property
    def render_config(self):
        """
        Represents the ``render-config`` detail endpoint.

        Returns a DetailEndpoint object that is the interface for
        viewing response from the render-config endpoint.

        :returns: :py:class:`.DetailEndpoint`

        :Examples:

        >>> vm = nb.virtualization.virtual_machines.get(123)
        >>> vm.render_config.create()
        """
        return DetailEndpoint(self, "render-config")
