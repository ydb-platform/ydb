# -*- coding: utf-8 -*-
"""High level wrapper for GPIB resources.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

from enum import Enum
from time import perf_counter
from typing import Tuple

from .. import attributes, constants
from ..attributes import Attribute
from .messagebased import ControlRenMixin, MessageBasedResource
from .resource import Resource


class GPIBCommand(bytes, Enum):
    """GPIB commands to use in send_command.

    Please note that group_execute_trigger provide a high level interface to
    perform a trigger addressed to multiple instrument on the bus.

    See: https://www.ni.com/docs/en-US/bundle/gpib-for-labview-nxg/page/ieee-488-command-messages.html
    for the complete list of GPIB commands.

    """

    #: GTL: GO TO LOCAL affects only addressed devices
    go_to_local = GTL = b"\x01"

    #: SDC: SELECTED DEVICE CLEAR
    selected_device_clear = SDC = b"\x04"

    #: PPC: PARALLEL POLL CONFIGURE
    parallel_poll_configure = PPC = b"\x05"

    #: PPU: PARALLEL POLL UNCONFIGURE
    parallel_poll_unconfigure = PPU = b"\x15"

    #: GET: GROUP EXECUTE TRIGGER
    group_execute_trigger = GET = b"\x08"

    #: TCT: TAKE CONTROL
    take_control = TCT = b"\x09"

    #: LLO: LOCAL LOCKOUT
    local_lockout = LLO = b"\x11"

    #: DCL: DEVICE CLEAR
    device_clear = DCL = b"\x14"

    #: SPE: SERIAL POLL ENABLE
    serial_poll_enable = SPE = b"\x18"

    #: SPD: SERIAL POLL DISABLE
    serial_poll_disable = SPD = b"\x19"

    #: CFE: CONFIGURE ENABLE
    configure_enable = CFE = b"\x1f"

    #: UNT: UNTALK
    untalk = UNT = b"\x5f"

    #: UNL: UNLISTEN
    unlisten = UNL = b"\x3f"

    @staticmethod
    def talker(board_pad) -> bytes:
        """MTA: MY TALK ADDRESS."""
        return (0x40 + board_pad).to_bytes(1, "big")

    MTA = talker

    @staticmethod
    def listener(device_pad) -> bytes:
        """MLA: MY LISTEN ADDRESS."""
        return (0x20 + device_pad).to_bytes(1, "big")

    MLA = listener

    @staticmethod
    def secondary_address(device_sad) -> bytes:
        """MSA: MY SECONDARY ADDRESS

        For VISA SAD range from 1 to 31 and 0 is not SAD.

        WARNING: This is untested. We're assuming it to be 0x60 + device_sad,
        based on the pattern of 0x20 and 0x40 for the listener and talker addresses.

        """
        if device_sad == 0 or device_sad == constants.VI_NO_SEC_ADDR:
            return b""

        return (0x60 + device_sad).to_bytes(1, "big")

    MSA = secondary_address


class _GPIBMixin(ControlRenMixin):
    """Common attributes and methods of GPIB Instr and Interface."""

    #: Primary address of the GPIB device used by the given session.
    primary_address: Attribute[int] = attributes.AttrVI_ATTR_GPIB_PRIMARY_ADDR()

    #: Secondary address of the GPIB device used by the given session.
    secondary_address: Attribute[int] = attributes.AttrVI_ATTR_GPIB_SECONDARY_ADDR()

    #: Current state of the GPIB REN (Remote ENable) interface line.
    remote_enabled: Attribute[constants.LineState] = (
        attributes.AttrVI_ATTR_GPIB_REN_STATE()
    )


@Resource.register(constants.InterfaceType.gpib, "INSTR")
class GPIBInstrument(_GPIBMixin, MessageBasedResource):
    """Communicates with to devices of type GPIB::<primary address>[::INSTR]

    More complex resource names can be specified with the following grammar:
        GPIB[board]::primary address[::secondary address][::INSTR]

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Whether to unaddress the device (UNT and UNL) after each read or write operation.
    enable_unaddressing: Attribute[bool] = attributes.AttrVI_ATTR_GPIB_UNADDR_EN()

    #: Whether to use repeat addressing before each read or write operation.
    enable_repeat_addressing: Attribute[bool] = attributes.AttrVI_ATTR_GPIB_READDR_EN()

    def wait_for_srq(self, timeout: int = 25000) -> None:
        """Wait for a Service Request (SRQ) coming from the instrument.

        Note that this method is not ended when *another* instrument signals an
        SRQ, only *this* instrument.

        Parameters
        ----------
        timeout : int
            Maximum waiting time in milliseconds. Defaul: 25000 (milliseconds).
            None means waiting forever if necessary.

        """
        self.enable_event(
            constants.EventType.service_request, constants.EventMechanism.queue
        )

        if timeout and not (0 <= timeout <= 4294967295):
            raise ValueError("timeout value is invalid")

        starting_time = perf_counter()

        while True:
            if timeout is None:
                adjusted_timeout = constants.VI_TMO_INFINITE
            else:
                adjusted_timeout = int(
                    (starting_time + timeout / 1e3 - perf_counter()) * 1e3
                )
                if adjusted_timeout < 0:
                    adjusted_timeout = 0

            self.wait_on_event(constants.EventType.service_request, adjusted_timeout)
            if self.stb & 0x40:
                break

        self.discard_events(
            constants.EventType.service_request, constants.EventMechanism.queue
        )


@Resource.register(constants.InterfaceType.gpib, "INTFC")
class GPIBInterface(_GPIBMixin, MessageBasedResource):
    """Communicates with to devices of type GPIB::INTFC

    More complex resource names can be specified with the following grammar:
        GPIB[board]::INTFC

    Do not instantiate directly, use
    :meth:`pyvisa.highlevel.ResourceManager.open_resource`.

    """

    #: Is the specified GPIB interface currently the system controller.
    is_system_controller: Attribute[bool] = (
        attributes.AttrVI_ATTR_GPIB_SYS_CNTRL_STATE()
    )

    #: Is the specified GPIB interface currently CIC (Controller In Charge).
    is_controller_in_charge: Attribute[bool] = attributes.AttrVI_ATTR_GPIB_CIC_STATE()

    #: Current state of the GPIB ATN (ATtentioN) interface line.
    atn_state: Attribute[constants.LineState] = attributes.AttrVI_ATTR_GPIB_ATN_STATE()

    #: Current state of the GPIB NDAC (Not Data ACcepted) interface line.
    ndac_state: Attribute[constants.LineState] = (
        attributes.AttrVI_ATTR_GPIB_NDAC_STATE()
    )

    #: Is the GPIB interface currently addressed to talk or listen, or is not addressed.
    address_state: Attribute[constants.LineState] = (
        attributes.AttrVI_ATTR_GPIB_ADDR_STATE()
    )

    def group_execute_trigger(
        self, *resources: GPIBInstrument
    ) -> Tuple[int, constants.StatusCode]:
        """

        Parameters
        ----------
        resources : GPIBInstrument
            GPIB resources to which to send the group trigger.

        Returns
        -------
        int
            Number of bytes written as part of sending the GPIB commands.
        constants.StatusCode
            Return value of the library call.

        """
        board_number = None
        for resource in resources:
            if not isinstance(resource, GPIBInstrument):
                raise ValueError(f"{resource!r} is not a GPIBInstrument")

            device_board = resource.interface_number
            if board_number is not None and board_number != device_board:
                raise ValueError(
                    f"{resource!r} is attached to board {device_board} but "
                    f"another device is attached to board {board_number}"
                )
            elif board_number is None:
                board_number = device_board

        if not self.is_controller_in_charge:
            self.send_ifc()

        # Broadcast board as talker and unlisten to all devices
        # Based on VISA address format for INTFC we cannot have a secondary address
        command = GPIBCommand.talker(self.primary_address) + GPIBCommand.unlisten

        for resource in resources:
            command += GPIBCommand.listener(
                resource.primary_address
            ) + GPIBCommand.secondary_address(resource.secondary_address)

        # send GET ('group execute trigger')
        command += GPIBCommand.group_execute_trigger

        return self.send_command(command)

    def send_command(self, data: bytes) -> Tuple[int, constants.StatusCode]:
        """Write GPIB command bytes on the bus.

        Corresponds to viGpibCommand function of the VISA library.

        Parameters
        ----------
        data : bytes
            Command to write.

        Returns
        -------
        int
            Number of bytes written
        constants.StatusCode
            Return value of the library call.

        """
        return self.visalib.gpib_command(self.session, data)

    def control_atn(self, mode: constants.ATNLineOperation) -> constants.StatusCode:
        """Specifies the state of the ATN line and the local active controller state.

        Corresponds to viGpibControlATN function of the VISA library.

        Parameters
        ----------
        mode : constants.ATNLineOperation
            Specifies the state of the ATN line and optionally the local active
             controller state.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return self.visalib.gpib_control_atn(self.session, mode)

    def pass_control(
        self, primary_address: int, secondary_address: int
    ) -> constants.StatusCode:
        """Tell a GPIB device to become controller in charge (CIC).

        Corresponds to viGpibPassControl function of the VISA library.

        Parameters
        ----------
        primary_address : int
            Primary address of the GPIB device to which you want to pass control.
        secondary_address : int
            Secondary address of the targeted GPIB device.
            If the targeted device does not have a secondary address,
            this parameter should contain the value Constants.NO_SEC_ADDR.

        Returns
        -------
        constants.StatusCode
            Return value of the library call.

        """
        return self.visalib.gpib_pass_control(
            self.session, primary_address, secondary_address
        )

    def send_ifc(self) -> constants.StatusCode:
        """Pulse the interface clear line (IFC) for at least 100 microseconds.

        Corresponds to viGpibSendIFC function of the VISA library.

        """
        return self.visalib.gpib_send_ifc(self.session)
