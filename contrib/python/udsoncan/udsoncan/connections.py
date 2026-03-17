import socket
import queue
import threading
import logging
import binascii
import sys
from abc import ABC, abstractmethod
import time
from typing import Union, Dict
import ctypes
import selectors

try:
    import can  # type:ignore
    _import_can_err = None
except Exception as e:
    _import_can_err = e

try:
    import isotp    # type:ignore
    _import_isotp_err = None
except Exception as e:
    _import_isotp_err = e

try:
    from udsoncan.j2534 import J2534, Protocol_ID, Error_ID, Ioctl_Flags, Ioctl_ID, SCONFIG_LIST
    _import_j2534_err = None
except Exception as e:
    _import_j2534_err = e

try:
    from aioisotp.sync import SyncISOTPNetwork, SyncConnection  # type:ignore
    _import_aioisotp_err = None
except Exception as e:
    _import_aioisotp_err = e

from udsoncan.Request import Request
from udsoncan.Response import Response
from udsoncan.exceptions import TimeoutException


from typing import Optional, Tuple, cast


class BaseConnection(ABC):

    name: str
    logger: logging.Logger

    def __init__(self, name: Optional[str] = None):
        if name is None:
            self.name = 'Connection'
        else:
            self.name = 'Connection[%s]' % (name)

        self.logger = logging.getLogger(self.name)

    def send(self, data: Union[bytes, Request, Response], timeout: Optional[float] = None) -> None:
        """Sends data to the underlying transport protocol

        :param data: The data or object to send. If a Request or Response is given, the value returned by get_payload() will be sent.
        :type data: bytes, Request, Response

        :returns: None
        """
        self.check_connection_opened()

        if isinstance(data, Request) or isinstance(data, Response):
            payload = data.get_payload()
        else:
            payload = data

        self.logger.debug('Sending %d bytes : [%s]' % (len(payload), binascii.hexlify(payload).decode('ascii')))

        # backward compatibility
        if 'timeout' in self.specific_send.__code__.co_varnames:
            self.specific_send(payload, timeout=timeout)
        else:
            self.specific_send(payload)

    def check_connection_opened(self) -> None:
        if not self.is_open():
            raise RuntimeError(self.__class__.__name__ + ' is not opened')

    def wait_frame(self, timeout: Optional[float] = None, exception: bool = False) -> Optional[bytes]:
        """Waits for the reception of a frame of data from the underlying transport protocol

        :param timeout: The maximum amount of time to wait before giving up in seconds
        :type timeout: float
        :param exception: Boolean value indicating if this function may return exceptions.
                When ``True``, all exceptions may be raised, including ``TimeoutException``
                When ``False``, all exceptions will be logged as ``DEBUG`` and ``None`` will be returned.
        :type exception: bool

        :returns: Received data
        :rtype: bytes or None
        """
        self.check_connection_opened()

        try:
            frame = self.specific_wait_frame(timeout=timeout)
        except Exception as e:
            self.logger.debug('No data received: [%s] - %s ' % (e.__class__.__name__, str(e)))

            if exception == True:
                raise
            else:
                frame = None

        if frame is not None:
            self.logger.debug('Received %d bytes : [%s]' % (len(frame), binascii.hexlify(frame).decode('ascii')))
        return frame

    def __enter__(self):
        return self

    @abstractmethod
    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        """The implementation of the send method.

        :param payload: Data to send
        :type payload: bytes

        :returns: None
        """
        pass

    @abstractmethod
    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        """The implementation of the ``wait_frame`` method. 

        :param timeout: The maximum amount of time to wait before giving up
        :type timeout: float

        :returns: Received data
        :rtype: bytes or None
        """
        pass

    @abstractmethod
    def open(self) -> "BaseConnection":
        """ Set up the connection object. 

        :returns: None
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """ Close the connection object

        :returns: None
        """
        pass

    @abstractmethod
    def empty_rxqueue(self) -> None:
        """ Empty all unread data in the reception buffer.

        :returns: None
        """
        pass

    @abstractmethod
    def is_open(self) -> bool:
        """ Tells if the connection is open.

        :returns: bool
        """
        pass

    def __exit__(self, type, value, traceback):
        pass


class SocketConnection(BaseConnection):
    """
    Sends and receives data through a socket.

    :param sock: The socket to use. This socket must be bound and ready to use. Only ``send()`` and ``recv()`` will be called by this Connection
    :type sock: socket.socket
    :param bufsize: Maximum buffer size of the socket, this value is passed to ``recv()``
    :type bufsize: int
    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string

    """

    rxqueue: "queue.Queue[bytes]"
    exit_requested: bool
    opened: bool
    rxthread: Optional[threading.Thread]
    sock: socket.socket
    bufsize: int

    def __init__(self, sock: socket.socket, bufsize: int = 4095, name: Optional[str] = None):
        BaseConnection.__init__(self, name)

        self.rxqueue = queue.Queue()
        self.exit_requested = False
        self.opened = False
        self.rxthread = None
        self.sock = sock
        self.bufsize = bufsize

    def open(self) -> "SocketConnection":
        self.exit_requested = False
        self.rxthread = threading.Thread(target=self.rxthread_task, daemon=True)
        self.rxthread.start()
        self.opened = True
        self.logger.info('Connection opened')
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def rxthread_task(self) -> None:
        sel = selectors.DefaultSelector()
        sel.register(self.sock, selectors.EVENT_READ)
        while not self.exit_requested:
            try:
                events = sel.select(timeout=0.2)
                if events:
                    data = self.sock.recv(self.bufsize)
                    if data is not None:
                        self.rxqueue.put(data)
            except Exception:
                self.exit_requested = True

    def close(self) -> None:
        self.exit_requested = True
        if self.rxthread is not None:
            self.rxthread.join()
        self.opened = False
        self.logger.info('Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        # timeout not used for generic sockets
        self.sock.send(payload)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        try:
            return self.rxqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutException("Did not received frame in time (timeout=%s sec)" % timeout)

    def empty_rxqueue(self) -> None:
        while not self.rxqueue.empty():
            self.rxqueue.get()


class IsoTPSocketConnection(BaseConnection):
    """
    Sends and receives data through an ISO-TP socket. Makes cleaner code than SocketConnection but offers no additional functionality.
    The `can-isotp module <https://github.com/pylessard/python-can-isotp>`_ must be installed in order to use this connection

    :param interface: The can interface to use (example: ``can0``)
    :type interface: string
    :param address: The address used to bind the the socket. Before 1.21, txid/rxid were needed here, this has changed with v1.21
    :type address: ``isotp.Address`` or ``isotp.AsymmetricAddress`` 
    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string
    :param tpsock: An optional ISO-TP socket to use instead of creating one.
    :type tpsock: isotp.socket

    """

    interface: str
    address: Union["isotp.Address", "isotp.AsymmetricAddress"]
    rxqueue: "queue.Queue[bytes]"
    exit_requested: bool
    opened: bool

    def __init__(self,
                 interface: str,
                 address: Union["isotp.Address", "isotp.AsymmetricAddress"],
                 name: Optional[str] = None,
                 tpsock: Optional["isotp.socket"] = None,
                 **kwargs
                 ):

        BaseConnection.__init__(self, name)

        self.interface = interface
        self.address = address
        self.rxqueue = queue.Queue()
        self.exit_requested = False
        self.opened = False

        # Lives with the past.
        if 'txid' in kwargs or 'rxid' in kwargs:
            raise RuntimeError(
                "Provide an isotp.Address to the %s. The interface has changed in a non-backward compatible way and this is now required." % self.__class__.__name__)

        if tpsock is None:
            if 'isotp' not in sys.modules:
                if _import_isotp_err is None:
                    raise ImportError('isotp module is not loaded')
                else:
                    raise _import_isotp_err
            self.tpsock = isotp.socket(timeout=0.1)
        else:
            self.tpsock = tpsock

    def open(self) -> "IsoTPSocketConnection":
        self.tpsock.bind(self.interface, address=self.address)
        self.exit_requested = False
        self.rxthread = threading.Thread(target=self.rxthread_task, daemon=True)
        self.rxthread.start()
        self.opened = True
        self.logger.info('Connection opened')
        return self

    def __enter__(self) -> "IsoTPSocketConnection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.tpsock.bound

    def rxthread_task(self) -> None:
        sel = selectors.DefaultSelector()
        sel.register(self.tpsock._socket, selectors.EVENT_READ)
        while not self.exit_requested:
            try:
                events = sel.select(timeout=0.2)
                if events:
                    data = self.tpsock.recv()
                    if data is not None:
                        self.rxqueue.put(data)
            except Exception:
                self.exit_requested = True

    def close(self) -> None:
        self.exit_requested = True
        if self.rxthread is not None:
            self.rxthread.join()
        self.tpsock.close()
        self.opened = False
        self.logger.info('Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        self.tpsock.send(payload)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        try:
            return self.rxqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutException("Did not received ISOTP frame in time (timeout=%s sec)" % timeout)

    def empty_rxqueue(self) -> None:
        while not self.rxqueue.empty():
            self.rxqueue.get()


class IsoTPConnection(IsoTPSocketConnection):
    """
    Same as :class:`IsoTPSocketConnection <udsoncan.connections.IsoTPSocketConnection.Session>`. Exists only for backward compatibility. 
    """
    pass


class QueueConnection(BaseConnection):
    """
    Sends and receives data using 2 Python native queues.

    - ``MyConnection.fromuserqueue`` : Data read from this queue when ``wait_frame`` is called
    - ``MyConnection.touserqueue`` : Data written to this queue when ``send`` is called

    :param mtu: Optional maximum frame size. Messages will be truncated to this size
    :type mtu: int
    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string
    """

    fromuserqueue: "queue.Queue[bytes]"
    touserqueue: "queue.Queue[bytes]"
    opened: bool
    mtu: int

    def __init__(self, name: Optional[str] = None, mtu: int = 4095):
        BaseConnection.__init__(self, name)

        self.fromuserqueue = queue.Queue()  # Client reads from this queue. Other end is simulated
        self.touserqueue = queue.Queue()  # Client writes to this queue. Other end is simulated
        self.opened = False
        self.mtu = mtu

    def open(self) -> "QueueConnection":
        self.opened = True
        self.logger.info('Connection opened')
        return self

    def __enter__(self) -> "QueueConnection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def close(self) -> None:
        self.empty_rxqueue()
        self.empty_txqueue()
        self.opened = False
        self.logger.info('Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        if self.mtu is not None:
            if len(payload) > self.mtu:
                self.logger.warning("Truncating payload to be set to a length of %d" % (self.mtu))
                payload = payload[0:self.mtu]

        self.touserqueue.put(payload, block=True, timeout=timeout)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        frame = None
        try:
            frame = self.fromuserqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutException("Did not receive frame from user queue in time (timeout=%s sec)" % timeout)

        if self.mtu is not None:
            if frame is not None and len(frame) > self.mtu:
                self.logger.warning("Truncating received payload to a length of %d" % (self.mtu))
                frame = frame[0:self.mtu]

        return frame

    def empty_rxqueue(self) -> None:
        while not self.fromuserqueue.empty():
            self.fromuserqueue.get()

    def empty_txqueue(self) -> None:
        while not self.touserqueue.empty():
            self.touserqueue.get()


class PythonIsoTpConnection(BaseConnection):
    """
    Sends and receives data using a `can-isotp <https://github.com/pylessard/python-can-isotp>`_ Python module which is a Python implementation of the IsoTp transport protocol
    which can be coupled with `python-can <https://python-can.readthedocs.io>`_ module to interract with CAN hardware

    `can-isotp <https://github.com/pylessard/python-can-isotp>`_ must be installed in order to use this connection.

    See an :ref:`example<example_using_python_can>`

    :param isotp_layer: The IsoTP Transport layer object coming from the ``isotp`` module.
    :type isotp_layer: :class:`isotp.TransportLayer<isotp.TransportLayer>`

    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string

    """

    subconn: Union["PythonIsoTpV1Connection", "PythonIsoTpV2Connection"]

    def __init__(self,
                 isotp_layer: Union["isotp.TransportLayerLogic", "isotp.TransportLayer"],
                 name: Optional[str] = None
                 ):
        BaseConnection.__init__(self, name)
        import isotp
        if hasattr(isotp, '_major_version_'):    # isotp v2.x
            if isotp._major_version_ == 2:
                if isinstance(isotp_layer, isotp.TransportLayer):   # This one has its own thread
                    self.subconn = PythonIsoTpV2Connection(isotp_layer, name)
                elif isinstance(isotp_layer, isotp.TransportLayerLogic):    # Need to create a thread for this one
                    self.subconn = PythonIsoTpV1Connection(isotp_layer, name)
                else:
                    raise ValueError("Invalid isotp layer object")
            else:
                raise NotImplementedError("Unsupported isotp version")
        else:   # isotp v1.x
            self.subconn = PythonIsoTpV1Connection(isotp_layer, name)

    def open(self) -> "PythonIsoTpConnection":
        self.subconn.open()
        return self

    def __enter__(self) -> "PythonIsoTpConnection":
        self.subconn.__enter__()
        return self

    def __exit__(self, type, value, traceback) -> None:
        return self.subconn.__exit__(type, value, traceback)

    def is_open(self) -> bool:
        return self.subconn.is_open()

    def close(self) -> None:
        return self.subconn.close()

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        self.subconn.specific_send(payload, timeout)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        return self.subconn.specific_wait_frame(timeout)

    def empty_rxqueue(self) -> None:
        return self.subconn.empty_rxqueue()

    def empty_txqueue(self) -> None:
        return self.subconn.empty_txqueue()


class PythonIsoTpV2Connection(BaseConnection):

    isotp_layer: "isotp.TransportLayer"
    opened: bool

    def __init__(self, isotp_layer: "isotp.TransportLayer", name: Optional[str] = None):
        BaseConnection.__init__(self, name)
        self.opened = False
        self.isotp_layer = isotp_layer

        assert isinstance(self.isotp_layer, isotp.TransportLayer), 'isotp_layer must be a valid isotp.TransportLayer '

    def open(self) -> "PythonIsoTpV2Connection":
        self.isotp_layer.start()
        self.opened = True
        self.logger.info('Connection opened')
        return self

    def __enter__(self) -> "PythonIsoTpV2Connection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def close(self) -> None:
        self.isotp_layer.stop()
        self.empty_rxqueue()
        self.empty_txqueue()
        self.opened = False
        self.logger.info('Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        self.isotp_layer.send(payload, send_timeout=timeout)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        frame = self.isotp_layer.recv(block=True, timeout=timeout)
        if frame is None:
            raise TimeoutException("Did not receive IsoTP frame from the Transport layer in time (timeout=%s sec)" % timeout)

        return bytes(frame)

    def empty_rxqueue(self) -> None:
        self.isotp_layer.stop_receiving()
        self.isotp_layer.clear_rx_queue()

    def empty_txqueue(self) -> None:
        self.isotp_layer.stop_sending()
        self.isotp_layer.clear_tx_queue()


class PythonIsoTpV1Connection(BaseConnection):
    toIsoTPQueue: "queue.Queue[bytes]"
    fromIsoTPQueue: "queue.Queue[bytes]"
    rxthread: Optional[threading.Thread]
    exit_requested: bool
    opened: bool
    isotp_layer: "isotp.TransportLayerLogic"

    def __init__(self, isotp_layer: "isotp.TransportLayerLogic", name: Optional[str] = None):
        BaseConnection.__init__(self, name)
        self.toIsoTPQueue = queue.Queue()
        self.fromIsoTPQueue = queue.Queue()
        self.rxthread = None
        self.exit_requested = False
        self.opened = False
        self.isotp_layer = isotp_layer

        # isotp v1 TransportLayer == isotpv2.TransportLayerLogic
        if hasattr(isotp, 'TransportLayerLogic'):
            assert isinstance(self.isotp_layer, isotp.TransportLayerLogic), 'isotp_layer must be a valid isotp.TransportLayerLogic'
        else:
            assert isinstance(self.isotp_layer, isotp.TransportLayer), 'isotp_layer must be a valid isotp.isotp.TransportLayer'

    def open(self) -> "PythonIsoTpV1Connection":
        self.exit_requested = False
        self.rxthread = threading.Thread(target=self.rxthread_task, daemon=True)
        self.rxthread.start()
        self.opened = True
        self.logger.info('Connection opened')
        return self

    def __enter__(self) -> "PythonIsoTpV1Connection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def close(self) -> None:
        self.empty_rxqueue()
        self.empty_txqueue()
        self.exit_requested = True
        if self.rxthread is not None:
            self.rxthread.join()
        self.isotp_layer.reset()
        self.opened = False
        self.logger.info('Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None):
        self.toIsoTPQueue.put(bytearray(payload))  # isotp.protocol.TransportLayer uses byte array. udsoncan is strict on bytes format

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        try:
            frame = self.fromIsoTPQueue.get(block=True, timeout=timeout)
            # isotp.protocol.TransportLayer uses bytearray. udsoncan is strict on bytes format
            return bytes(frame)
        except queue.Empty:
            raise TimeoutException("Did not receive IsoTP frame from the Transport layer in time (timeout=%s sec)" % timeout)

    def empty_rxqueue(self) -> None:
        while not self.fromIsoTPQueue.empty():
            self.fromIsoTPQueue.get()

    def empty_txqueue(self) -> None:
        while not self.toIsoTPQueue.empty():
            self.toIsoTPQueue.get()

    def rxthread_task(self) -> None:
        while not self.exit_requested:
            try:
                while not self.toIsoTPQueue.empty():
                    self.isotp_layer.send(self.toIsoTPQueue.get())

                self.isotp_layer.process()

                while self.isotp_layer.available():
                    self.fromIsoTPQueue.put(self.isotp_layer.recv())

                time.sleep(self.isotp_layer.sleep_time())

            except Exception as e:
                self.exit_requested = True
                self.logger.error(str(e))


class J2534Connection(BaseConnection):
    """
    Sends and receives data through a J2534 Interface. 
    A windows DLL and a J2534 interface must be installed in order to use this connection

    :param windll: The path to the windows DLL for the J2534 interface (example: 'C:/Program Files{x86}../../openport 2.0/op20pt32.dll')
    :type interface: string
    :param rxid: The reception CAN id
    :type rxid: int 
    :param txid: The transmission CAN id
    :type txid: int
    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string
    :param debug: This will enable windows debugging mode in the dll (see tactrix doc for additional information)
    :type debug: boolean
    :param protocol: CAN protocol
    :type protocol: Protocol_ID
    :param baudrate: Operation bauderate
    :type baudrate: int

    """

    interface: "J2534"
    protocol: "Protocol_ID"
    baudrate: int
    result: "Error_ID"
    firmwareVersion: "ctypes.Array[ctypes.c_char]"
    dllVersion: "ctypes.Array[ctypes.c_char]"
    apiVersion: "ctypes.Array[ctypes.c_char]"
    rxqueue: "queue.Queue[bytes]"
    exit_requested: bool
    opened: bool

    def __init__(self,
                 windll: str,
                 rxid: int,
                 txid: int,
                 name: Optional[str] = None,
                 debug: bool = False,
                 protocol = None,
                 baudrate = 500000,
                 ):
        BaseConnection.__init__(self, name)

        self.protocol = protocol if protocol else Protocol_ID.ISO15765
        self.baudrate = baudrate
        self.debug = debug

        try:
            # Set up a J2534 interface using the DLL provided
            self.interface = J2534(windll=windll, rxid=rxid, txid=txid)

            # Open the interface (connect to the DLL)
            self.result, self.devID = self.interface.PassThruOpen()
        except FileNotFoundError:
            raise RuntimeError('DLL not found')
        except OSError as e:
            if e.errno in [0x16, 0xe06d7363]:
                raise RuntimeError('J2534 Device busy')
            exception_str = type(e).__name__
            if e.errno is not None:
                exception_str += ', %s' % e.errno
            raise RuntimeError(exception_str)

        self.log_last_operation("PassThruOpen", with_raise=True)

        if debug:
            self.result = self.interface.PassThruIoctl(0,
                                                       Ioctl_Flags.TX_IOCTL_SET_DLL_DEBUG_FLAGS,
                                                       SCONFIG_LIST([(0, Ioctl_Flags.TX_IOCTL_DLL_DEBUG_FLAG_J2534_CALLS.value)])
                                                       )
            self.log_last_operation("PassThruIoctl SET_DLL_DEBUG")

        # Get the firmeware and DLL version etc, mainly for debugging output
        self.result, self.firmwareVersion, self.dllVersion, self.apiVersion = self.interface.PassThruReadVersion(self.devID)
        self.logger.info("J2534 FirmwareVersion: " + str(self.firmwareVersion.value) + ", dllVersoin: " +
                         str(self.dllVersion.value) + ", apiVersion" + str(self.apiVersion.value))

        # get the channel ID of the interface (used for subsequent communication)
        self.result, self.channelID = self.interface.PassThruConnect(self.devID, self.protocol.value, self.baudrate)
        self.log_last_operation("PassThruConnect", with_raise=True)

        configs = [
            (Ioctl_ID.DATA_RATE.value, self.baudrate),
            (Ioctl_ID.LOOPBACK.value, 0),
        ]
        if self.protocol in [Protocol_ID.ISO9141, Protocol_ID.ISO14230]:
            configs += [
                (Ioctl_ID.P1_MAX.value, 40),
                (Ioctl_ID.P3_MIN.value, 110),
                (Ioctl_ID.P4_MIN.value, 10),
                (Ioctl_ID.TIDLE.value,  300),
                (Ioctl_ID.TWUP.value,   50),
                (Ioctl_ID.TINL.value,   25),
            ]
        elif self.protocol in [Protocol_ID.ISO15765]:
            configs += [
                (Ioctl_ID.ISO15765_BS.value, 0x20),
                (Ioctl_ID.ISO15765_STMIN.value, 0),
            ]

        self.result = self.interface.PassThruIoctl(self.channelID, Ioctl_ID.SET_CONFIG, SCONFIG_LIST(configs))
        self.log_last_operation("PassThruIoctl SET_CONFIG")

        self.result = self.interface.PassThruIoctl(self.channelID, Ioctl_ID.CLEAR_MSG_FILTERS)
        self.log_last_operation("PassThruIoctl CLEAR_MSG_FILTERS")

        # Set the filters and clear the read buffer (filters will be set based on tx/rxids)
        self.result = self.interface.PassThruStartMsgFilter(self.channelID, self.protocol.value)
        self.log_last_operation("PassThruStartMsgFilter")

        self.result = self.interface.PassThruIoctl(self.channelID, Ioctl_ID.CLEAR_RX_BUFFER)
        self.log_last_operation("PassThruIoctl CLEAR_RX_BUFFER")

        self.result = self.interface.PassThruIoctl(self.channelID, Ioctl_ID.CLEAR_TX_BUFFER)
        self.log_last_operation("PassThruIoctl CLEAR_TX_BUFFER")

        self.rxqueue = queue.Queue()
        self.exit_requested = False
        self.opened = False

    def open(self) -> "J2534Connection":
        self.exit_requested = False
        self.interfaceSemaphore = threading.Semaphore()
        self.rxthread = threading.Thread(target=self.rxthread_task, daemon=True)
        self.rxthread.start()
        self.opened = True
        self.logger.info('J2534 Connection opened')
        return self

    def __enter__(self) -> "J2534Connection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def rxthread_task(self) -> None:
        while not self.exit_requested:
            self.interfaceSemaphore.acquire()
            try:
                result, data, numMessages = self.interface.PassThruReadMsgs(self.channelID, self.protocol.value, pNumMsgs=1)
                if data is not None:
                    self.rxqueue.put(data)
            except Exception:
                self.logger.critical("Exiting J2534 rx thread")
                self.exit_requested = True
            self.interfaceSemaphore.release()
            time.sleep(0.001)

    def log_last_operation(self, exec_method: str, with_raise = False) -> None:
        if self.result != Error_ID.ERR_SUCCESS:
            res, pErrDescr = self.interface.PassThruGetLastError()
            err = "J2534 %s: %s (%s)" % (exec_method, pErrDescr, self.result)
            self.logger.error(err)
            if with_raise:
                raise RuntimeError(err)
            return

        elif self.debug:
            self.logger.debug("J2534 %s: OK" % (exec_method))

    def close(self) -> None:
        self.opened = False
        self.exit_requested = True
        self.rxthread.join()

        self.result = self.interface.PassThruDisconnect(self.channelID)
        self.log_last_operation('PassThruDisconnect')

        self.interface.PassThruClose(self.devID)
        self.log_last_operation('PassThruClose')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None):
        timeout = 0 if timeout is None else timeout

        # Fix for avoid ERR_CONCURRENT_API_CALL. Stop reading
        self.interfaceSemaphore.acquire()
        self.result = self.interface.PassThruWriteMsgs(self.channelID, payload, self.protocol.value, Timeout=int(timeout * 1000))
        self.log_last_operation('PassThruWriteMsgs', with_raise=True)
        self.interfaceSemaphore.release()

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        try:
            return self.rxqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutException("Did not received response from J2534 RxQueue (timeout=%s sec)" % timeout)

    def empty_rxqueue(self) -> None:
        while not self.rxqueue.empty():
            self.rxqueue.get()

    def read_vbatt(self, digits=1) -> float:
        vbatt = ctypes.POINTER(ctypes.c_int32)()

        self.result = self.interface.PassThruIoctl(self.channelID, Ioctl_ID.READ_VBATT, None, vbatt)
        self.log_last_operation("PassThruIoctl READ_VBATT")

        value = ctypes.cast(vbatt, ctypes.c_void_p).value

        return round(value / 1000, digits) if value else 0


class FakeConnection(BaseConnection):
    """
    Sends and receives static data defined in a local dict. 
    Used so that an application can be tested without a live can network
    """

    rxqueue: "queue.Queue[bytes]"
    exit_requested: bool
    opened: bool
    ResponseData: Dict[bytes, bytes]

    def __init__(self, name=None, debug=False, *args, **kwargs):

        BaseConnection.__init__(self, name)

        self.rxqueue = queue.Queue()

        self.exit_requested = False
        self.opened = False

        self.ResponseData = {b'\x10\x03': b'\x50\x03\x12\x23\x34\x45',
                             b'\x22\xf1\x90\xf1\x89\xf1\x91\xf8\x06\xf1\xa3': b'\x22\xf1\x90\xf1\x89\xf1\x91\xf8\x06\xf1\xa3'}

    def open(self) -> "FakeConnection":
        self.opened = True
        self.logger.info('Fake Connection opened')
        return self

    def __enter__(self) -> "FakeConnection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()

    def is_open(self) -> bool:
        return self.opened

    def close(self) -> None:
        self.exit_requested = True
        self.opened = False
        self.logger.info('Fake Connection closed')

    def specific_send(self, payload: bytes, timeout: Optional[float] = None):
        self.rxqueue.put(self.ResponseData[payload])

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()

        try:
            return self.rxqueue.get(block=True, timeout=timeout)
        except queue.Empty:
            raise TimeoutException("Did not received response from J2534 RxQueue (timeout=%s sec)" % timeout)

    def empty_rxqueue(self) -> None:
        while not self.rxqueue.empty():
            self.rxqueue.get()


class SyncAioIsotpConnection(BaseConnection):
    """
    A wrapper for aioisotp sync variant

    `aioisotp <https://github.com/christiansandberg/aioisotp>`_ must be installed in order to use this connection.

    See an :ref:`example<example_using_aioisotp>`

    :param rxid: The reception CAN id
    :type rxid: int

    :param txid: The transmission CAN id
    :type txid: int

    :param name: This name is included in the logger name so that its output can be redirected. The logger name will be ``Connection[<name>]``
    :type name: string

    :param args: Optional parameters list passed to aioisotp binding method.
    :type args: list

    :param kwargs: Optional parameters dictionary passed to aioisotp binding method.
    :type kwargs: dict
    """

    network: "SyncISOTPNetwork"
    opened: bool
    rx_id: int
    tx_id: int
    conn: Optional["SyncConnection"]

    def __init__(self, rx_id: int, tx_id: int, name: Optional[str] = None, *args, **kwargs):
        BaseConnection.__init__(self, name)
        self.network = SyncISOTPNetwork(*args, **kwargs)
        self.opened = False
        self.rx_id = rx_id
        self.tx_id = tx_id
        self.conn = None
        self.opened = False

    def specific_send(self, payload: bytes, timeout: Optional[float] = None) -> None:
        self.check_connection_opened()
        assert self.conn is not None

        self.conn.send(payload)

    def specific_wait_frame(self, timeout: Optional[float] = None) -> Optional[bytes]:
        self.check_connection_opened()
        assert self.conn is not None

        frame = cast(Optional[bytes], self.conn.recv(timeout))

        if frame is None and timeout:
            raise TimeoutException("Did not received frame in time (timeout=%s sec)" % timeout)

        return frame

    def open(self) -> "SyncAioIsotpConnection":
        self.network.open()
        self.conn = self.network.create_sync_connection(self.rx_id, self.tx_id)
        self.opened = True
        self.logger.info("Connection opened")
        return self

    def close(self) -> None:
        self.network.close()
        self.opened = False
        self.logger.info("Connection closed")

    def empty_rxqueue(self) -> None:
        if self.conn is not None:
            self.conn.empty()

    def is_open(self) -> bool:
        return self.conn is not None and self.opened

    def __enter__(self) -> "SyncAioIsotpConnection":
        return self

    def __exit__(self, type, value, traceback) -> None:
        self.close()
