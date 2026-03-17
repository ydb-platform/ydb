"""Implementation of a Threaded Modbus Server."""
from __future__ import annotations

import asyncio
import traceback

from pymodbus.exceptions import ModbusIOException, NoSuchSlaveException
from pymodbus.logging import Log
from pymodbus.pdu.pdu import ExceptionResponse
from pymodbus.transaction import TransactionManager
from pymodbus.transport import CommParams, ModbusProtocol


class ServerRequestHandler(TransactionManager):
    """Handle client connection."""

    def __init__(self, owner, trace_packet, trace_pdu, trace_connect):
        """Initialize."""
        params = CommParams(
            comm_name="server",
            comm_type=owner.comm_params.comm_type,
            reconnect_delay=0.0,
            reconnect_delay_max=0.0,
            timeout_connect=0.0,
            host=owner.comm_params.source_address[0],
            port=owner.comm_params.source_address[1],
            handle_local_echo=owner.comm_params.handle_local_echo,
        )
        self.server = owner
        self.framer = self.server.framer(self.server.decoder)
        self.running = False
        super().__init__(
            params,
            self.framer,
            0,
            True,
            trace_packet,
            trace_pdu,
            trace_connect,
        )

    def callback_new_connection(self) -> ModbusProtocol:
        """Call when listener receive new connection request."""
        raise RuntimeError("callback_new_connection should never be called")

    def callback_connected(self) -> None:
        """Call when connection is succcesfull."""
        super().callback_connected()
        slaves = self.server.context.slaves()
        if self.server.broadcast_enable:
            if 0 not in slaves:
                slaves.append(0)

    def callback_disconnected(self, call_exc: Exception | None) -> None:
        """Call when connection is lost."""
        super().callback_disconnected(call_exc)
        try:
            if call_exc is None:
                Log.debug(
                    "Handler for stream [{}] has been canceled", self.comm_params.comm_name
                )
            else:
                Log.debug(
                    "Client Disconnection {} due to {}",
                    self.comm_params.comm_name,
                    call_exc,
                )
            self.running = False
        except Exception as exc:  # pylint: disable=broad-except
            Log.error(
                "Datastore unable to fulfill request: {}; {}",
                exc,
                traceback.format_exc(),
            )

    def callback_data(self, data: bytes, addr: tuple | None = None) -> int:
        """Handle received data."""
        try:
            used_len = super().callback_data(data, addr)
        except ModbusIOException:
            response = ExceptionResponse(
                40,
                exception_code=ExceptionResponse.ILLEGAL_FUNCTION
            )
            self.server_send(response, 0)
            return(len(data))
        if self.last_pdu:
            self.loop.call_soon(self.handle_later)
        return used_len

    def handle_later(self):
        """Change sync (async not allowed in call_soon) to async."""
        asyncio.run_coroutine_threadsafe(self.handle_request(), self.loop)

    async def handle_request(self):
        """Handle request."""
        broadcast = False
        if not self.last_pdu:
            return
        try:
            if self.server.broadcast_enable and not self.last_pdu.dev_id:
                broadcast = True
                # if broadcasting then execute on all slave contexts,
                # note response will be ignored
                for dev_id in self.server.context.slaves():
                    response = await self.last_pdu.update_datastore(self.server.context[dev_id])
            else:
                context = self.server.context[self.last_pdu.dev_id]
                response = await self.last_pdu.update_datastore(context)

        except NoSuchSlaveException:
            Log.error("requested slave does not exist: {}", self.last_pdu.dev_id)
            if self.server.ignore_missing_slaves:
                return  # the client will simply timeout waiting for a response
            response = ExceptionResponse(0x00, ExceptionResponse.GATEWAY_NO_RESPONSE)
        except Exception as exc:  # pylint: disable=broad-except
            Log.error(
                "Datastore unable to fulfill request: {}; {}",
                exc,
                traceback.format_exc(),
            )
            response = ExceptionResponse(0x00, ExceptionResponse.SLAVE_FAILURE)
        # no response when broadcasting
        if not broadcast:
            response.transaction_id = self.last_pdu.transaction_id
            response.dev_id = self.last_pdu.dev_id
            self.server_send(response, self.last_addr)

    def server_send(self, pdu, addr):
        """Send message."""
        if not pdu:
            Log.debug("Skipping sending response!!")
        else:
            self.pdu_send(pdu, addr=addr)
