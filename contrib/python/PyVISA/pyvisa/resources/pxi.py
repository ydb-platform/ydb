# -*- coding: utf-8 -*-
"""High level wrapper for pxi resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .. import attributes, constants
from ..attributes import Attribute
from .registerbased import RegisterBasedResource
from .resource import Resource


@Resource.register(constants.InterfaceType.pxi, "BACKPLANE")
class PXIBackplane(RegisterBasedResource):
    """Communicates with to devices of type PXI[interface]::BACKPLANE

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Manufacturer name.
    manufacturer_name: Attribute[str] = attributes.AttrVI_ATTR_MANF_NAME()

    #: Model name of the device.
    model_name: Attribute[str] = attributes.AttrVI_ATTR_MODEL_NAME()


class PXICommon(RegisterBasedResource):
    """Common parent class for PXI INSTR and MEMACC resources."""

    #: Number of elements by which to increment the source offset after a transfer.
    source_increment: Attribute[int] = attributes.AttrVI_ATTR_SRC_INCREMENT()

    #: Number of elements by which to increment the destination offset after a transfer.
    destination_increment: Attribute[int] = attributes.AttrVI_ATTR_DEST_INCREMENT()

    #: Should I/O accesses use DMA (True) or Programmed I/O (False).
    allow_dma: Attribute[bool] = attributes.AttrVI_ATTR_DMA_ALLOW_EN()


@Resource.register(constants.InterfaceType.pxi, "INSTR")
class PXIInstrument(PXICommon):
    """Communicates with to devices of type PXI::<device>[::INSTR]

    More complex resource names can be specified with the following grammar:
        PXI[bus]::device[::function][::INSTR]
    or:
        PXI[interface]::bus-device[.function][::INSTR]
    or:
        PXI[interface]::CHASSISchassis number::SLOTslot number[::FUNCfunction][::INSTR]

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


@Resource.register(constants.InterfaceType.pxi, "MEMACC")
class PXIMemory(PXICommon):
    """Communicates with to devices of type PXI[interface]::MEMACC

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """
