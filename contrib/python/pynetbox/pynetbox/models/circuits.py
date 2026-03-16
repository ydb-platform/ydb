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

from pynetbox.core.response import PathableRecord, Record


class Circuits(Record):
    def __str__(self):
        return self.cid


class CircuitTerminations(PathableRecord):
    """Circuit Termination Record with cable path tracing support.

    Circuit terminations support cable path tracing via the paths() method,
    which returns all cable paths traversing this circuit termination point.
    """

    def __str__(self):
        return self.circuit.cid


class VirtualCircuits(Record):
    """Virtual Circuit Record.

    Virtual circuits represent L2VPN-like connections delivered across
    one or more physical circuits.
    """

    def __str__(self):
        return self.cid


class VirtualCircuitTerminations(PathableRecord):
    """Virtual Circuit Termination Record with cable path tracing support.

    Virtual circuit terminations support cable path tracing via the paths()
    method, which returns all cable paths traversing this termination point.
    """

    def __str__(self):
        return self.virtual_circuit.cid
