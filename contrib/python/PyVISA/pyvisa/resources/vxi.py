# -*- coding: utf-8 -*-
"""High level wrapper for VXI resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .. import attributes, constants
from ..attributes import Attribute
from .registerbased import RegisterBasedResource
from .resource import Resource


@Resource.register(constants.InterfaceType.vxi, "BACKPLANE")
class VXIBackplane(Resource):
    """Communicates with to devices of type VXI::BACKPLANE

    More complex resource names can be specified with the following grammar:
        VXI[board][::VXI logical address]::BACKPLANE

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """


class VXICommon(RegisterBasedResource):
    """Common parent class for VXI INSTR and MEMACC resources."""

    #: Number of elements by which to increment the source offset after a transfer.
    source_increment: Attribute[int] = attributes.AttrVI_ATTR_SRC_INCREMENT()

    #: Number of elements by which to increment the destination offset after a transfer.
    destination_increment: Attribute[int] = attributes.AttrVI_ATTR_DEST_INCREMENT()

    #: Should I/O accesses use DMA (True) or Programmed I/O (False).
    allow_dma: Attribute[bool] = attributes.AttrVI_ATTR_DMA_ALLOW_EN()


@Resource.register(constants.InterfaceType.vxi, "INSTR")
class VXIInstrument(VXICommon):
    """Communicates with to devices of type VXI::VXI logical address[::INSTR]

    More complex resource names can be specified with the following grammar:
        VXI[board]::VXI logical address[::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Manufacturer name.
    manufacturer_name: Attribute[str] = attributes.AttrVI_ATTR_MANF_NAME()

    #: Manufacturer identification number of the device.
    manufacturer_id: Attribute[int] = attributes.AttrVI_ATTR_MANF_ID()

    #: Model name of the device.
    model_name: Attribute[str] = attributes.AttrVI_ATTR_MODEL_NAME()

    #: Model code for the device.
    model_code: Attribute[int] = attributes.AttrVI_ATTR_MODEL_CODE()

    #: Whether the device is 488.2 compliant.
    is_4882_compliant: Attribute[bool] = attributes.AttrVI_ATTR_4882_COMPLIANT()

    #: Should END be asserted during the transfer of the last byte of the buffer.
    send_end: Attribute[bool] = attributes.AttrVI_ATTR_SEND_END_EN()

    #: IO protocol to use. See the attribute definition for more details.
    io_protocol: Attribute[constants.IOProtocol] = attributes.AttrVI_ATTR_IO_PROT()


@Resource.register(constants.InterfaceType.vxi, "MEMACC")
class VXIMemory(VXICommon):
    """Communicates with to devices of type VXI[board]::MEMACC

    More complex resource names can be specified with the following grammar:
        VXI[board]::MEMACC

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """
