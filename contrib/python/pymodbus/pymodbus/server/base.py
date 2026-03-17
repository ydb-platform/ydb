"""Implementation of a Threaded Modbus Server."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import suppress

from pymodbus.datastore import ModbusServerContext
from pymodbus.device import ModbusControlBlock, ModbusDeviceIdentification
from pymodbus.framer import FRAMER_NAME_TO_CLASS, FramerType
from pymodbus.logging import Log
from pymodbus.pdu import DecodePDU, ModbusPDU
from pymodbus.transport import CommParams, ModbusProtocol

from .requesthandler import ServerRequestHandler


class ModbusBaseServer(ModbusProtocol):
    """Common functionality for all server classes."""

    active_server: ModbusBaseServer | None

    def __init__(  # pylint: disable=too-many-arguments
        self,
        params: CommParams,
        context: ModbusServerContext | None,
        ignore_missing_slaves: bool,
        broadcast_enable: bool,
        identity: ModbusDeviceIdentification | None,
        framer: FramerType,
        trace_packet: Callable[[bool, bytes], bytes] | None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None,
        trace_connect: Callable[[bool], None] | None,
        custom_pdu: list[type[ModbusPDU]] | None,
    ) -> None:
        """Initialize base server."""
        super().__init__(
            params,
            True,
        )
        self.loop = asyncio.get_running_loop()
        self.decoder = DecodePDU(True)
        if custom_pdu:
            for func in custom_pdu:
                self.decoder.register(func)
        self.context = context or ModbusServerContext()
        self.control = ModbusControlBlock()
        self.ignore_missing_slaves = ignore_missing_slaves
        self.broadcast_enable = broadcast_enable
        self.trace_packet = trace_packet
        self.trace_pdu = trace_pdu
        self.trace_connect = trace_connect
        self.handle_local_echo = False
        if isinstance(identity, ModbusDeviceIdentification):
            self.control.Identity.update(identity)

        self.framer = FRAMER_NAME_TO_CLASS[framer]
        self.serving: asyncio.Future = asyncio.Future()
        ModbusBaseServer.active_server = self

    def callback_new_connection(self):
        """Handle incoming connect."""
        if self.trace_connect:
            self.trace_connect(True)
        return ServerRequestHandler(
            self,
            self.trace_packet,
            self.trace_pdu,
            self.trace_connect
        )

    async def shutdown(self):
        """Close server."""
        if not self.serving.done():
            self.serving.set_result(True)
        self.close()

    async def serve_forever(self, *, background: bool = False):
        """Start endless loop."""
        if self.transport:
            raise RuntimeError(
                "Can't call serve_forever on an already running server object"
            )
        await self.listen()
        Log.info("Server listening.")
        if not background:
            with suppress(asyncio.exceptions.CancelledError):
                await self.serving
            Log.info("Server graceful shutdown.")

    def callback_connected(self) -> None:
        """Call when connection is succcesfull."""
        raise RuntimeError("callback_new_connection should never be called")

    def callback_disconnected(self, exc: Exception | None) -> None:
        """Call when connection is lost."""
        raise RuntimeError("callback_disconnected should never be called")

    def callback_data(self, data: bytes, addr: tuple | None = None) -> int:
        """Handle received data."""
        raise RuntimeError("callback_data should never be called")
