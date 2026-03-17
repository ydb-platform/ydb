# -*- coding: utf-8 -*-
"""High level wrapper for VXI resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .. import constants
from .registerbased import RegisterBasedResource


@RegisterBasedResource.register(constants.InterfaceType.firewire, "INSTR")
class FirewireInstrument(RegisterBasedResource):
    """Communicates with devices of type VXI::VXI logical address[::INSTR]

    More complex resource names can be specified with the following grammar:
        VXI[board]::VXI logical address[::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """
