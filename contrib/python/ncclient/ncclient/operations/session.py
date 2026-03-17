# Copyright 2009 Shikhar Bhushan
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

"Session-related NETCONF operations"

from ncclient.xml_ import *

from ncclient.operations.rpc import RPC

class CloseSession(RPC):

    "`close-session` RPC. The connection to NETCONF server is also closed."

    def request(self):
        "Request graceful termination of the NETCONF session, and also close the transport."
        ret = self._request(new_ele("close-session"))
        self.session.close()
        return ret


class KillSession(RPC):

    "`kill-session` RPC."

    def request(self, session_id):
        """Force the termination of a NETCONF session (not the current one!)

        *session_id* is the session identifier of the NETCONF session to be terminated as a string
        """
        node = new_ele("kill-session")
        sub_ele(node, "session-id").text = session_id
        return self._request(node)
