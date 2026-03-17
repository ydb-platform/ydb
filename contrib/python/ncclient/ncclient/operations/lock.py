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

"Locking-related NETCONF operations"

from ncclient.xml_ import *

from ncclient.operations.rpc import RaiseMode, RPC

# TODO: parse session-id from a lock-denied error, and raise a tailored exception?

class Lock(RPC):

    "`lock` RPC"


    def request(self, target="candidate"):
        """Allows the client to lock the configuration system of a device.

        *target* is the name of the configuration datastore to lock
        """
        node = new_ele("lock")
        sub_ele(sub_ele(node, "target"), target)
        return self._request(node)


class Unlock(RPC):

    "`unlock` RPC"

    def request(self, target="candidate"):
        """Release a configuration lock, previously obtained with the lock operation.

        *target* is the name of the configuration datastore to unlock
        """
        node = new_ele("unlock")
        sub_ele(sub_ele(node, "target"), target)
        return self._request(node)


class LockContext:

    """A context manager for the :class:`Lock` / :class:`Unlock` pair of RPC's.

    Any `rpc-error` will be raised as an exception.

    Initialise with (:class:`Session <ncclient.transport.Session>`) instance and lock target.
    """

    def __init__(self, session, device_handler, target):
        self.session = session
        self.target = target
        self.device_handler = device_handler

    def __enter__(self):
        Lock(self.session, self.device_handler, raise_mode=RaiseMode.ERRORS).request(self.target)
        return self

    def __exit__(self, *args):
        Unlock(self.session, self.device_handler, raise_mode=RaiseMode.ERRORS).request(self.target)
        return False
