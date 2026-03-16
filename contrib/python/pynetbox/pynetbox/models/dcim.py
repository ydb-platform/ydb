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

from pynetbox.core.endpoint import (
    DetailEndpoint,
    RODetailEndpoint,
    ROMultiFormatDetailEndpoint,
)
from pynetbox.core.query import Request
from pynetbox.core.response import JsonField, PathableRecord, Record
from pynetbox.models.circuits import Circuits
from pynetbox.models.ipam import IpAddresses


class TraceableRecord(Record):
    def _build_termination_data(self, termination_list):
        terminations_data = []
        for hop_item_data in termination_list:
            return_obj_class = self._get_obj_class(hop_item_data["url"])
            terminations_data.append(
                return_obj_class(hop_item_data, self.endpoint.api, self.endpoint)
            )

        return terminations_data

    def trace(self):
        req = Request(
            key=str(self.id) + "/trace",
            base=self.endpoint.url,
            token=self.api.token,
            http_session=self.api.http_session,
        ).get()

        ret = []
        for a_terminations_data, cable_data, b_terminations_data in req:
            ret.append(self._build_termination_data(a_terminations_data))
            if not cable_data:
                ret.append(cable_data)
            else:
                return_obj_class = self._get_obj_class(cable_data["url"])
                ret.append(
                    return_obj_class(cable_data, self.endpoint.api, self.endpoint)
                )
            ret.append(self._build_termination_data(b_terminations_data))

        return ret


class DeviceTypes(Record):
    def __str__(self):
        return self.model


class Devices(Record):
    """Devices Object.

    Represents a device response from netbox.

    ## Attributes
    * **primary_ip, ip4, ip6** (list): Tells __init__ in Record() to
        take the `primary_ip` field's value from the API
        response and return an initialized list of IpAddress
        objects
    * **device_type** (obj): Tells __init__ in Record() to take the
        `device_type` field's value from the API response and
        return an initialized DeviceType object
    """

    has_details = True
    device_type = DeviceTypes
    primary_ip = IpAddresses
    primary_ip4 = IpAddresses
    primary_ip6 = IpAddresses
    local_context_data = JsonField
    config_context = JsonField

    @property
    def napalm(self):
        """Represents the ``napalm`` detail endpoint.

        Returns a DetailEndpoint object that is the interface for
        viewing response from the napalm endpoint.

        ## Returns
        DetailEndpoint object.

        ## Examples

        ```python
        device = nb.ipam.devices.get(123)
        device.napalm.list(method='get_facts')
        # {"get_facts": {"interface_list": ["ge-0/0/0"]}}
        ```
        """
        return RODetailEndpoint(self, "napalm")

    @property
    def render_config(self):
        """Represents the ``render-config`` detail endpoint.

        Returns a DetailEndpoint object that is the interface for
        viewing response from the render-config endpoint.

        ## Returns
        DetailEndpoint object.

        ## Examples

        ```python
        device = nb.ipam.devices.get(123)
        device.render_config.create()
        ```
        """
        return DetailEndpoint(self, "render-config")


class InterfaceConnections(Record):
    def __str__(self):
        return self.interface_a.name


class InterfaceConnection(Record):
    def __str__(self):
        return self.interface.name


class Interfaces(TraceableRecord):
    interface_connection = InterfaceConnection


class PowerFeeds(TraceableRecord):
    pass


class PowerOutlets(TraceableRecord):
    device = Devices


class PowerPorts(TraceableRecord):
    device = Devices


class ConsolePorts(TraceableRecord):
    device = Devices


class ConsoleServerPorts(TraceableRecord):
    device = Devices


class RackReservations(Record):
    def __str__(self):
        return self.description


class VirtualChassis(Record):
    master = Devices


class RUs(Record):
    device = Devices


class FrontPorts(PathableRecord):
    device = Devices


class RearPorts(PathableRecord):
    device = Devices


class Racks(Record):
    @property
    def units(self):
        """Represents the ``units`` detail endpoint.

        Returns a DetailEndpoint object that is the interface for
        viewing response from the units endpoint.

        ## Returns
        DetailEndpoint object.

        ## Examples

        ```python
        rack = nb.dcim.racks.get(123)
        rack.units.list()
        # {"get_facts": {"interface_list": ["ge-0/0/0"]}}
        ```
        """
        return RODetailEndpoint(self, "units", custom_return=RUs)

    @property
    def elevation(self):
        """Represents the ``elevation`` detail endpoint.

        Returns a multi-format endpoint supporting both JSON and SVG responses.
        The elevation endpoint provides rack unit information and can render
        graphical elevation views.

        ## Returns
        ROMultiFormatDetailEndpoint object supporting JSON and SVG formats.

        ## Examples

        ```python
        rack = nb.dcim.racks.get(123)

        # Get rack units as JSON (list of RU objects)
        rack.elevation.list()

        # Get elevation as SVG diagram
        svg = rack.elevation.list(render='svg')
        ```
        """
        return ROMultiFormatDetailEndpoint(self, "elevation", custom_return=RUs)


class Termination(Record):
    def __str__(self):
        # hacky check to see if we're a circuit termination to
        # avoid another call to NetBox because of a non-existent attr
        # in self.name
        if "circuit" in str(self.url):
            return self.circuit.cid

        return self.name

    device = Devices
    circuit = Circuits


class Cables(Record):
    def __str__(self):
        if len(self.a_terminations) == 1 and len(self.b_terminations) == 1:
            return "{} <> {}".format(self.a_terminations[0], self.b_terminations[0])
        return "Cable #{}".format(self.id)
