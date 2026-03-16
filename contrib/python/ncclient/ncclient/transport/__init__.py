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

"Transport layer"

import sys

from ncclient.transport.session import Session, SessionListener, NetconfBase
from ncclient.transport.errors import *


def __getattr__(name):
    if name == 'SSHSession':
        from ncclient.transport.ssh import SSHSession
        return SSHSession
    elif name == 'TLSSession':
        from ncclient.transport.tls import TLSSession
        return TLSSession
    elif name == 'UnixSocketSession':
        #
        # Windows does not support Unix domain sockets; assume all other platforms do
        #
        if sys.platform != 'win32':
            try:
                from ncclient.transport.unixSocket import UnixSocketSession
                return UnixSocketSession
            except Exception:
                pass
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")


__all__ = [
    'Session',
    'SessionListener',
    'TransportError',
    'AuthenticationError',
    'SessionCloseError',
    'NetconfBase',
    'SSHError',
    'SSHUnknownHostError',
    'SSHSession',
    'TLSSession',
    'UnixSocketSession',
]

#
# Windows does not support Unix domain sockets; assume all other platforms do
#
if sys.platform != 'win32':
    try:
        from ncclient.transport.unixSocket import UnixSocketSession
        __all__.append('UnixSocketSession')
    except Exception:
        pass

#
# check if ssh-python is installed
#
try:
    import ssh
except ImportError:
    pass
else:
    from ncclient.transport.libssh import LibSSHSession
    __all__.append('LibSSHSession')
