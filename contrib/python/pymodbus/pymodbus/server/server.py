"""Implementation of a Threaded Modbus Server."""
from __future__ import annotations

from collections.abc import Callable

from pymodbus.datastore import ModbusServerContext
from pymodbus.device import ModbusDeviceIdentification
from pymodbus.framer import FramerType
from pymodbus.pdu import ModbusPDU
from pymodbus.transport import CommParams, CommType

from .base import ModbusBaseServer


class ModbusTcpServer(ModbusBaseServer):
    """A modbus threaded tcp socket server.

    .. tip::
        Remember to call serve_forever to start server.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        context: ModbusServerContext,
        *,
        framer=FramerType.SOCKET,
        identity: ModbusDeviceIdentification | None = None,
        address: tuple[str, int] = ("", 502),
        ignore_missing_slaves: bool = False,
        broadcast_enable: bool = False,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
        custom_pdu: list[type[ModbusPDU]] | None = None,
    ):
        """Initialize the socket server.

        If the identify structure is not passed in, the ModbusControlBlock
        uses its own empty structure.

        :param context: The ModbusServerContext datastore
        :param framer: The framer strategy to use
        :param identity: An optional identify structure
        :param address: An optional (interface, port) to bind to.
        :param ignore_missing_slaves: True to not send errors on a request
                        to a missing slave
        :param broadcast_enable: True to treat dev_id 0 as broadcast address,
                        False to treat 0 as any other dev_id
        :param trace_packet: Called with bytestream received/to be sent
        :param trace_pdu: Called with PDU received/to be sent
        :param trace_connect: Called when connected/disconnected
        :param custom_pdu: list of ModbusPDU custom classes
        """
        params = getattr(
            self,
            "tls_setup",
            CommParams(
                comm_type=CommType.TCP,
                comm_name="server_listener",
                reconnect_delay=0.0,
                reconnect_delay_max=0.0,
                timeout_connect=0.0,
            ),
        )
        params.source_address = address
        super().__init__(
            params,
            context,
            ignore_missing_slaves,
            broadcast_enable,
            identity,
            framer,
            trace_packet,
            trace_pdu,
            trace_connect,
            custom_pdu,
        )


class ModbusTlsServer(ModbusTcpServer):
    """A modbus threaded tls socket server.

    .. tip::
        Remember to call serve_forever to start server.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        context: ModbusServerContext,
        *,
        framer=FramerType.TLS,
        identity: ModbusDeviceIdentification | None = None,
        address: tuple[str, int] = ("", 502),
        sslctx=None,
        certfile=None,
        keyfile=None,
        password=None,
        ignore_missing_slaves=False,
        broadcast_enable=False,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
        custom_pdu: list[type[ModbusPDU]] | None = None,
    ):
        """Overloaded initializer for the socket server.

        If the identify structure is not passed in, the ModbusControlBlock
        uses its own empty structure.

        :param context: The ModbusServerContext datastore
        :param framer: The framer strategy to use
        :param identity: An optional identify structure
        :param address: An optional (interface, port) to bind to.
        :param sslctx: The SSLContext to use for TLS (default None and auto
                       create)
        :param certfile: The cert file path for TLS (used if sslctx is None)
        :param keyfile: The key file path for TLS (used if sslctx is None)
        :param password: The password for for decrypting the private key file
        :param ignore_missing_slaves: True to not send errors on a request
                        to a missing slave
        :param broadcast_enable: True to treat dev_id 0 as broadcast address,
                        False to treat 0 as any other dev_id
        :param trace_packet: Called with bytestream received/to be sent
        :param trace_pdu: Called with PDU received/to be sent
        :param trace_connect: Called when connected/disconnected
        :param custom_pdu: list of ModbusPDU custom classes
        """
        self.tls_setup = CommParams(
            comm_type=CommType.TLS,
            comm_name="server_listener",
            reconnect_delay=0.0,
            reconnect_delay_max=0.0,
            timeout_connect=0.0,
            sslctx=CommParams.generate_ssl(
                True, certfile, keyfile, password, sslctx=sslctx
            ),
        )
        super().__init__(
            context,
            framer=framer,
            identity=identity,
            address=address,
            ignore_missing_slaves=ignore_missing_slaves,
            broadcast_enable=broadcast_enable,
            trace_packet=trace_packet,
            trace_pdu=trace_pdu,
            trace_connect=trace_connect,
            custom_pdu=custom_pdu,
        )


class ModbusUdpServer(ModbusBaseServer):
    """A modbus threaded udp socket server.

    .. tip::
        Remember to call serve_forever to start server.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        context: ModbusServerContext,
        *,
        framer=FramerType.SOCKET,
        identity: ModbusDeviceIdentification | None = None,
        address: tuple[str, int] = ("", 502),
        ignore_missing_slaves: bool = False,
        broadcast_enable: bool = False,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
        custom_pdu: list[type[ModbusPDU]] | None = None,
    ):
        """Overloaded initializer for the socket server.

        If the identify structure is not passed in, the ModbusControlBlock
        uses its own empty structure.

        :param context: The ModbusServerContext datastore
        :param framer: The framer strategy to use
        :param identity: An optional identify structure
        :param address: An optional (interface, port) to bind to.
        :param ignore_missing_slaves: True to not send errors on a request
                            to a missing slave
        :param broadcast_enable: True to treat dev_id 0 as broadcast address,
                            False to treat 0 as any other dev_id
        :param trace_packet: Called with bytestream received/to be sent
        :param trace_pdu: Called with PDU received/to be sent
        :param trace_connect: Called when connected/disconnected
        :param custom_pdu: list of ModbusPDU custom classes
        """
        # ----------------
        params = CommParams(
            comm_type=CommType.UDP,
            comm_name="server_listener",
            source_address=address,
            reconnect_delay=0.0,
            reconnect_delay_max=0.0,
            timeout_connect=0.0,
        )
        super().__init__(
            params,
            context,
            ignore_missing_slaves,
            broadcast_enable,
            identity,
            framer,
            trace_packet,
            trace_pdu,
            trace_connect,
            custom_pdu,
        )


class ModbusSerialServer(ModbusBaseServer):
    """A modbus threaded serial socket server.

    .. tip::
        Remember to call serve_forever to start server.
    """

    def __init__(
        self,
        context: ModbusServerContext,
        *,
        framer: FramerType = FramerType.RTU,
        ignore_missing_slaves: bool = False,
        identity: ModbusDeviceIdentification | None = None,
        broadcast_enable: bool = False,
        trace_packet: Callable[[bool, bytes], bytes] | None = None,
        trace_pdu: Callable[[bool, ModbusPDU], ModbusPDU] | None = None,
        trace_connect: Callable[[bool], None] | None = None,
        custom_pdu: list[type[ModbusPDU]] | None = None,
        **kwargs
    ):
        """Initialize the socket server.

        If the identity structure is not passed in, the ModbusControlBlock
        uses its own empty structure.
        :param context: The ModbusServerContext datastore
        :param framer: The framer strategy to use, default FramerType.RTU
        :param identity: An optional identify structure
        :param port: The serial port to attach to
        :param stopbits: The number of stop bits to use
        :param bytesize: The bytesize of the serial messages
        :param parity: Which kind of parity to use
        :param baudrate: The baud rate to use for the serial device
        :param timeout: The timeout to use for the serial device
        :param handle_local_echo: (optional) Discard local echo from dongle.
        :param ignore_missing_slaves: True to not send errors on a request
                            to a missing slave
        :param broadcast_enable: True to treat dev_id 0 as broadcast address,
                            False to treat 0 as any other dev_id
        :param reconnect_delay: reconnect delay in seconds
        :param trace_packet: Called with bytestream received/to be sent
        :param trace_pdu: Called with PDU received/to be sent
        :param trace_connect: Called when connected/disconnected
        :param custom_pdu: list of ModbusPDU custom classes
        """
        params = CommParams(
            comm_type=CommType.SERIAL,
            comm_name="server_listener",
            reconnect_delay=kwargs.get("reconnect_delay", 2),
            reconnect_delay_max=0.0,
            timeout_connect=kwargs.get("timeout", 3),
            source_address=(kwargs.get("port", 0), 0),
            bytesize=kwargs.get("bytesize", 8),
            parity=kwargs.get("parity", "N"),
            baudrate=kwargs.get("baudrate", 19200),
            stopbits=kwargs.get("stopbits", 1),
            handle_local_echo=kwargs.get("handle_local_echo", False)
        )
        super().__init__(
            params,
            context,
            ignore_missing_slaves,
            broadcast_enable,
            identity,
            framer,
            trace_packet,
            trace_pdu,
            trace_connect,
            custom_pdu,
        )
        self.handle_local_echo = kwargs.get("handle_local_echo", False)
