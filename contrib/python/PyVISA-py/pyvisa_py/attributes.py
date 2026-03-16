# -*- coding: utf-8 -*-
"""Additional Attributes for specific use with the pyvisa-py package.

For additional information and VISA attributes see pyvisa.constants

:copyright: 2014-2024 by PyVISA-py Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.
"""

from pyvisa import constants
from pyvisa.attributes import AttrVI_ATTR_TCPIP_KEEPALIVE as former_keepalive


class AttrVI_ATTR_TCPIP_KEEPALIVE(former_keepalive):
    """Requests that a TCP/IP provider enable the use of keep-alive packets.

    Altering the standard PyVISA attribute to also work on INSTR sessions as
    they are using sockets in pyvisa-py as well.

    After the system detects that a connection was dropped, VISA returns a lost
    connection error code on subsequent I/O calls on the session. The time required
    for the system to detect that the connection was dropped is dependent on the
    system and is not settable.

    """

    resources = [
        (constants.InterfaceType.tcpip, "SOCKET"),
        (constants.InterfaceType.tcpip, "INSTR"),
        (constants.InterfaceType.vicp, "INSTR"),
    ]
