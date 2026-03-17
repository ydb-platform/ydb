# Copyright 2h009 Shikhar Bhushan
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'Power-control operations'

from ncclient.xml_ import *

from ncclient.operations.rpc import RPC

PC_URN = "urn:liberouter:params:xml:ns:netconf:power-control:1.0"

class PoweroffMachine(RPC):

    "*poweroff-machine* RPC (flowmon)"

    DEPENDS = ["urn:liberouter:param:netconf:capability:power-control:1.0"]
    
    def request(self):
        return self._request(new_ele(qualify("poweroff-machine", PC_URN)))

class RebootMachine(RPC):

    "*reboot-machine* RPC (flowmon)"

    DEPENDS = ["urn:liberouter:params:netconf:capability:power-control:1.0"]

    def request(self):
        return self._request(new_ele(qualify("reboot-machine", PC_URN)))
