import socket as socket_module
import os
import isotp.address

from typing import TYPE_CHECKING, Optional, Union


if TYPE_CHECKING:
    from . import opts

mtu = 4095


def check_support() -> None:
    if not hasattr(socket_module, 'CAN_ISOTP'):
        if os.name == 'nt':
            raise NotImplementedError("This module cannot be used on Windows")
        else:
            raise NotImplementedError(
                "Your version of Python does not offer support for CAN ISO-TP protocol. Support have been added since Python 3.7 on Linux build > 2.6.15.")


class flags:

    LISTEN_MODE = 0x001
    """Puts the socket in Listen mode, which prevents transmission of data"""

    EXTEND_ADDR = 0x002
    """When set, an address extension byte (set in socket general options) will be added to each payload sent. Unless RX_EXT_ADDR is also set, this value will be expected for reception as well"""

    TX_PADDING = 0x004
    """Enables padding of transmitted data with a byte set in the socket general options"""

    RX_PADDING = 0x008
    """ Indicates that data padding is possible in reception. Must be set for CHK_PAD_LEN and CHK_PAD_DATA to have an effect"""

    CHK_PAD_LEN = 0x010
    """ Makes the socket validate the padding length of the CAN message"""

    CHK_PAD_DATA = 0x020
    """ Makes the socket validate the padding bytes of the CAN message"""

    HALF_DUPLEX = 0x040
    """ Sets the socket in half duplex mode, forcing transmission and reception to happen sequentially """

    FORCE_TXSTMIN = 0x080
    """Forces the socket to use the separation time sets in general options, overriding stmin value received in flow control frames."""

    FORCE_RXSTMIN = 0x100
    """ Forces the socket to ignore any message received faster than stmin given in the flow control frame"""

    RX_EXT_ADDR = 0x200
    """ When sets, a different extended address can be used for reception than for transmission."""

    WAIT_TX_DONE = 0x400
    """ When set, we wait for tx completion to make sure the PDU is completely passed to the CAN netdevice queue."""


class LinkLayerProtocol:
    CAN = 16
    """ Internal structure size of a CAN 2.0 frame"""

    CAN_FD = 72
    """ Internal structure size of a CAN FD frame"""


class socket:
    """
    A IsoTP socket wrapper for easy configuration

    :param timeout: Passed down to the socket ``settimeout`` method. Control the blocking/non-blocking behavior of the socket 
    :type timeout: int | None

    """

    # We want that syntax isotp.socket.flags and isotp.socket.mtu
    # This is a workaround for sphinx autodoc that fails to load docstring for nested-class members
    flags = flags
    LinkLayerProtocol = LinkLayerProtocol

    interface: Optional[str]
    address: Optional[isotp.address.AbstractAddress]
    bound: bool
    closed: bool
    _socket: socket_module.socket

    def __init__(self, timeout: Optional[float] = None) -> None:
        check_support()
        from . import opts  # import only if required.
        self.interface = None
        self.address = None
        self.bound = False
        self.closed = False
        self._socket = socket_module.socket(socket_module.AF_CAN, socket_module.SOCK_DGRAM, socket_module.CAN_ISOTP)
        if timeout is not None and timeout > 0:
            self.settimeout(timeout)

    def settimeout(self, value: Optional[float]) -> None:
        self._socket.settimeout(value)

    def gettimeout(self) -> Optional[float]:
        return self._socket.gettimeout()

    def send(self, data: bytes, flags: int = 0) -> int:
        if not self.bound:
            raise RuntimeError("bind() must be called before using the socket")

        return self._socket.send(data, flags)

    def recv(self, bufsize: int = mtu, flags: int = 0) -> bytes:
        if not self.bound:
            raise RuntimeError("bind() must be called before using the socket")
        return self._socket.recv(bufsize, flags)

    def set_ll_opts(self,
                    mtu: Optional[int] = None,
                    tx_dl: Optional[int] = None,
                    tx_flags: Optional[int] = None
                    ) -> "opts.LinkLayerOpts":
        """ 
        Sets the link layer options. Default values are set to work with CAN 2.0. Link layer may be configure to work in CAN FD.
        Values of ``None`` will leave the parameter unchanged

        :param mtu: The internal CAN frame structure size. Possible values are defined in :class:`isotp.socket.LinkLayerProtocol<isotp.socket.LinkLayerProtocol>`
        :type mtu: int

        :param tx_dl: The CAN message payload length. For CAN 2.0, this value should be 8. For CAN FD, possible values are 8,12,16,20,24,32,48,64
        :type tx_dl: int

        :param tx_flags: Link layer flags.
        :type tx_flags: int

        :rtype: :class:`isotp.opts.LinkLayerOpts<isotp.opts.LinkLayerOpts>`

        """
        if self.bound:
            raise RuntimeError("Options must be set before calling bind()")

        return opts.LinkLayerOpts.write(self._socket,
                                        mtu=mtu,
                                        tx_dl=tx_dl,
                                        tx_flags=tx_flags)

    def set_opts(self,
                 optflag: Optional[int] = None,
                 frame_txtime: Optional[int] = None,
                 ext_address: Optional[int] = None,
                 txpad: Optional[int] = None,
                 rxpad: Optional[int] = None,
                 rx_ext_address: Optional[int] = None,
                 tx_stmin: Optional[int] = None) -> "opts.GeneralOpts":
        """

        Sets the general options of the socket. Values of ``None`` will leave the parameter unchanged

        :param optflag: A list of flags modifying the protocol behavior. Refer to :class:`socket.flags<isotp.socket.flags>`
        :type optflag: int

        :param frame_txtime: Frame transmission time (N_As/N_Ar) in nanoseconds.
        :type frame_txtime: int

        :param ext_address: The extended address to use. If not None, flags.EXTEND_ADDR will be set.
        :type ext_address: int

        :param txpad: The byte to use to pad the transmitted CAN messages. If not None, flags.TX_PADDING will be set
        :type txpad: int

        :param rxpad: The byte to use to pad the transmitted CAN messages. If not None, flags.RX_PADDING will be set
        :type rxpad: int

        :param rx_ext_address: The extended address to use in reception. If not None, flags.RX_EXT_ADDR will be set
        :type rx_ext_address: int

        :param tx_stmin: Sets the transmit separation time (time between consecutive frame) in nanoseconds. This value will override the value received through FlowControl frame. If not None, flags.FORCE_TXSTMIN will be set
        :type tx_stmin: int

        :rtype: :class:`isotp.opts.GeneralOpts<isotp.opts.GeneralOpts>`

        """

        if self.bound:
            raise RuntimeError("Options must be set before calling bind()")

        return opts.GeneralOpts.write(self._socket,
                                      optflag=optflag,
                                      frame_txtime=frame_txtime,
                                      ext_address=ext_address,
                                      txpad=txpad,
                                      rxpad=rxpad,
                                      rx_ext_address=rx_ext_address,
                                      tx_stmin=tx_stmin
                                      )

    def set_fc_opts(self, bs: Optional[int] = None, stmin: Optional[int] = None, wftmax: Optional[int] = None) -> "opts.FlowControlOpts":
        """   
        Sets the flow control options of the socket. Values of ``None`` will leave the parameter unchanged

        :param bs: The block size sent in the flow control message. Indicates the number of consecutive frame a sender can send before the socket sends a new flow control. A block size of 0 means that no additional flow control message will be sent (block size of infinity)
        :type bs: int

        :param stmin: The minimum separation time sent in the flow control message. Indicates the amount of time to wait between 2 consecutive frame. This value will be sent as is over CAN. Values from 1 to 127 means milliseconds. Values from 0xF1 to 0xF9 means 100us to 900us. 0 Means no timing requirements
        :type stmin: int

        :param wftmax: Maximum number of wait frame (flow control message with flow status=1) allowed before dropping a message. 0 means that wait frame are not allowed
        :type wftmax: int

        :rtype: :class:`isotp.opts.FlowControlOpts<isotp.opts.FlowControlOpts>`
        """
        if self.bound:
            raise RuntimeError("Options must be set before calling bind()")
        return opts.FlowControlOpts.write(self._socket, bs=bs, stmin=stmin, wftmax=wftmax)

    def get_ll_opts(self) -> "opts.LinkLayerOpts":
        return opts.LinkLayerOpts.read(self._socket)

    def get_opts(self) -> "opts.GeneralOpts":
        return opts.GeneralOpts.read(self._socket)

    def get_fc_opts(self) -> "opts.FlowControlOpts":
        return opts.FlowControlOpts.read(self._socket)

    def bind(self, interface: str, address: Union[isotp.Address, isotp.AsymmetricAddress]) -> None:
        """
        Binds the socket to an address. 

        :param interface: The network interface to use
        :type interface: string

        :param address: The address to bind to. 
        :type address: :class:`isotp.Address<isotp.Address>`
        """

        if not isinstance(interface, str):
            raise ValueError("interface must be a string")

        if not isinstance(address, (isotp.Address, isotp.AsymmetricAddress)):
            raise ValueError("address and instance of isotp.Address or isotp.AsymmetricAddress")

        if isinstance(address, isotp.AsymmetricAddress):
            if address.requires_rx_extension_byte() != address.requires_tx_extension_byte():
                # See https://github.com/hartkopp/can-isotp/issues/62
                raise ValueError("The IsoTP socket module does not support asymmetric addresses with inconsistent address_extension byte")

        self.interface = interface
        self.address = address

        # IsoTP sockets doesn't provide an interface to modify the target address type. We assume physical.
        # If functional is required, it Ids can be manually crafted in Normal / extended mode
        rxid = self.address.get_rx_arbitration_id(isotp.TargetAddressType.Physical)
        txid = self.address.get_tx_arbitration_id(isotp.TargetAddressType.Physical)

        if self.address.is_rx_29bits():
            rxid = (rxid & socket_module.CAN_EFF_MASK) | socket_module.CAN_EFF_FLAG
        else:
            rxid = rxid & socket_module.CAN_SFF_MASK

        if self.address.is_tx_29bits():
            txid = (txid & socket_module.CAN_EFF_MASK) | socket_module.CAN_EFF_FLAG
        else:
            txid = txid & socket_module.CAN_SFF_MASK

        if self.address.requires_tx_extension_byte() or self.address.requires_rx_extension_byte():
            o = self.get_opts()
            assert o.optflag is not None
            if self.address.requires_tx_extension_byte():
                o.optflag |= self.flags.EXTEND_ADDR
            if self.address.requires_rx_extension_byte():
                o.optflag |= self.flags.RX_EXT_ADDR

            self.set_opts(optflag=o.optflag, ext_address=self.address.get_tx_extension_byte(), rx_ext_address=self.address.get_rx_extension_byte())

        self._socket.bind((interface, rxid, txid))
        self.bound = True

    def fileno(self) -> int:
        """Returns the socket file descriptor"""
        return self._socket.fileno()

    def real_socket(self) -> socket_module.socket:
        """Return the real socket object hidden by the fake isotp socket object"""
        return self._socket

    def close(self) -> None:
        """Closes the socket"""
        self._socket.close()
        self.bound = False
        self.closed = True
        self.address = None

    def __delete__(self) -> None:
        if isinstance(self._socket, socket_module.socket):
            self._socket.close()

    def __repr__(self) -> str:
        if self.bound:
            assert self.address is not None
            return "<ISO-TP Socket: %s, %s>" % (self.interface, self.address.get_content_str())
        else:
            status = "Closed" if self.closed else "Unbound"
            return "<%s ISO-TP Socket at 0x%s>" % (status, hex(id(self)))
