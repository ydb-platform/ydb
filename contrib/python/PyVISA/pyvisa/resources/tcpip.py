# -*- coding: utf-8 -*-
"""High level wrapper for TCPIP resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .. import constants
from .messagebased import ControlRenMixin, MessageBasedResource
from .resource import Resource


@Resource.register(constants.InterfaceType.tcpip, "INSTR")
class TCPIPInstrument(ControlRenMixin, MessageBasedResource):
    """Communicates with to devices of type TCPIP::host address[::INSTR]

    More complex resource names can be specified with the following grammar:
        TCPIP[board]::host address[::LAN device name][::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    pass


@Resource.register(constants.InterfaceType.vicp, "INSTR")
class VICPInstrument(ControlRenMixin, MessageBasedResource):
    """Communicates with to devices of type VICP::host address[::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    pass


@Resource.register(constants.InterfaceType.tcpip, "SOCKET")
class TCPIPSocket(MessageBasedResource):
    """Communicates with to devices of type TCPIP::host address::port::SOCKET

    More complex resource names can be specified with the following grammar:
        TCPIP[board]::host address::port::SOCKET

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    pass


@Resource.register(constants.InterfaceType.prlgx_tcpip, "INTFC")
class PrlgxTCPIPIntfc(MessageBasedResource):
    """Communicates with to devices of type PRLGX-TCPIP::host address::port::INTFC

    More complex resource names can be specified with the following grammar:
        PRLGX-TCPIP[board]::host address::port::INTFC

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    pass
