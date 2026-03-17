# -*- coding: utf-8 -*-
"""High level wrapper for Serial resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from .. import attributes, constants
from ..attributes import Attribute
from .messagebased import MessageBasedResource


@MessageBasedResource.register(constants.InterfaceType.asrl, "INSTR")
class SerialInstrument(MessageBasedResource):
    """Communicates with devices of type ASRL<board>[::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Baud rate of the interface. The default value is 9600.
    baud_rate: Attribute[int] = attributes.AttrVI_ATTR_ASRL_BAUD()

    #: Number of data bits contained in each frame (from 5 to 8). The default value is 8.
    data_bits: Attribute[int] = attributes.AttrVI_ATTR_ASRL_DATA_BITS()

    #: Parity used with every frame transmitted and received. The default value is
    #: `constants.Parity.none` (VI_ASRL_PAR_NONE).
    parity: Attribute[constants.Parity] = attributes.AttrVI_ATTR_ASRL_PARITY()

    #: Number of stop bits used to indicate the end of a frame. The default value is
    #: `constants.StopBits.one` (VI_ASRL_STOP_ONE).
    stop_bits: Attribute[constants.StopBits] = attributes.AttrVI_ATTR_ASRL_STOP_BITS()

    #: Indicates the type of flow control used by the transfer mechanism. The default value
    #: is `constants.ControlFlow.none` (VI_ASRL_FLOW_NONE).
    flow_control: Attribute[constants.ControlFlow] = (
        attributes.AttrVI_ATTR_ASRL_FLOW_CNTRL()
    )

    #: Number of bytes available in the low- level I/O receive buffer.
    bytes_in_buffer: Attribute[int] = attributes.AttrVI_ATTR_ASRL_AVAIL_NUM()

    #: If set to True, NUL characters are discarded. The default is False.
    discard_null: Attribute[bool] = attributes.AttrVI_ATTR_ASRL_DISCARD_NULL()

    #: Manually control transmission. The default value is True.
    allow_transmit: Attribute[bool] = attributes.AttrVI_ATTR_ASRL_ALLOW_TRANSMIT()

    #: Method used to terminate read operations. The default value is
    #: `constants.SerialTermination.termination_char` (VI_ASRL_END_TERMCHAR).
    end_input: Attribute[constants.SerialTermination] = (
        attributes.AttrVI_ATTR_ASRL_END_IN()
    )

    #: Method used to terminate write operations. The default value is
    #: `constants.SerialTermination.none` (VI_ASRL_END_NONE) and terminates
    #: when all requested data is transferred or when an error occurs.
    end_output: Attribute[constants.SerialTermination] = (
        attributes.AttrVI_ATTR_ASRL_END_OUT()
    )

    #: Duration (in milliseconds) of the break signal. The default value is 250.
    break_length: Attribute[int] = attributes.AttrVI_ATTR_ASRL_BREAK_LEN()

    #: Manually control the assertion state of the break signal. The default state is
    #: `constants.LineState.unasserted` (VI_STATE_UNASSERTED).
    break_state: Attribute[constants.LineState] = (
        attributes.AttrVI_ATTR_ASRL_BREAK_STATE()
    )

    #: Character to be used to replace incoming characters that arrive with errors.
    #: The default character is '\0'.
    replace_char: Attribute[str] = attributes.AttrVI_ATTR_ASRL_REPLACE_CHAR()

    #: XOFF character used for XON/XOFF flow control (both directions).
    #: The default character is '0x13'.
    xoff_char: Attribute[str] = attributes.AttrVI_ATTR_ASRL_XOFF_CHAR()

    #: XON character used for XON/XOFF flow control (both directions).
    #: The default character is '0x11'.
    xon_char: Attribute[str] = attributes.AttrVI_ATTR_ASRL_XON_CHAR()


@MessageBasedResource.register(constants.InterfaceType.prlgx_asrl, "INTFC")
class PrlgxASRLIntfc(SerialInstrument):
    """Communicates with devices of type PRLGX-ASRL::serial device::INTFC

    More complex resource names can be specified with the following grammar:
        PRLGX-ASRL[board]::serial device::INTFC

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    pass
