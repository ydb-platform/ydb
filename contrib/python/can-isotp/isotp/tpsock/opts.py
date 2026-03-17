import socket as socket_module
import struct
from . import socket
from . import check_support

from typing import Optional

# opts cannot be imported when an isotp socket can't be created.
check_support()

flags = socket.flags


def assert_is_socket(s: socket_module.socket) -> None:
    if not isinstance(s, socket_module.socket):
        raise ValueError("Given value is not a socket.")


SOL_CAN_BASE = socket_module.SOL_CAN_BASE if hasattr(socket_module, 'SOL_CAN_BASE') else 100
SOL_CAN_ISOTP = SOL_CAN_BASE + socket_module.CAN_ISOTP
CAN_ISOTP_OPTS = 1
CAN_ISOTP_RECV_FC = 2
CAN_ISOTP_TX_STMIN = 3
CAN_ISOTP_RX_STMIN = 4
CAN_ISOTP_LL_OPTS = 5


class GeneralOpts:
    struct_size = 4 + 4 + 1 + 1 + 1 + 1

    optflag: Optional[int]
    frame_txtime: Optional[int]
    ext_address: Optional[int]
    txpad: Optional[int]
    rxpad: Optional[int]
    rx_ext_address: Optional[int]

    def __init__(self) -> None:
        self.optflag = None
        self.frame_txtime = None
        self.ext_address = None
        self.txpad = None
        self.rxpad = None
        self.rx_ext_address = None

    @classmethod
    def read(cls, s: socket_module.socket) -> "GeneralOpts":
        assert_is_socket(s)
        o = cls()
        opt = s.getsockopt(SOL_CAN_ISOTP, CAN_ISOTP_OPTS, cls.struct_size)

        (o.optflag, o.frame_txtime, o.ext_address, o.txpad, o.rxpad, o.rx_ext_address) = struct.unpack("=LLBBBB", opt)
        return o

    @classmethod
    def write(cls,
              s: socket_module.socket,
              optflag: Optional[int] = None,
              frame_txtime: Optional[int] = None,
              ext_address: Optional[int] = None,
              txpad: Optional[int] = None,
              rxpad: Optional[int] = None,
              rx_ext_address: Optional[int] = None,
              tx_stmin: Optional[int] = None
              ) -> "GeneralOpts":
        assert_is_socket(s)
        o = cls.read(s)
        assert o.optflag is not None

        if optflag is not None:
            if not isinstance(optflag, int) or optflag < 0 or optflag > 0xFFFFFFFF:
                raise ValueError("optflag must be a valid 32 unsigned integer")
            o.optflag = optflag

        if frame_txtime is not None:
            if not isinstance(frame_txtime, int) or frame_txtime < 0 or frame_txtime > 0xFFFFFFFF:
                raise ValueError("frame_txtime must be a valid 32 unsigned integer")
            o.frame_txtime = frame_txtime

        if ext_address is not None:
            if not isinstance(ext_address, int) or ext_address < 0 or ext_address > 0xFF:
                raise ValueError("ext_address must be a an integer between 0 and FF")
            o.ext_address = ext_address
            o.optflag |= flags.EXTEND_ADDR

        if txpad is not None:
            if not isinstance(txpad, int) or txpad < 0 or txpad > 0xFF:
                raise ValueError("txpad must be a an integer between 0 and FF")
            o.txpad = txpad
            o.optflag |= flags.TX_PADDING

        if rxpad is not None:
            if not isinstance(rxpad, int) or rxpad < 0 or rxpad > 0xFF:
                raise ValueError("rxpad must be a an integer between 0 and FF")
            o.rxpad = rxpad
            o.optflag |= flags.RX_PADDING

        if rx_ext_address is not None:
            if not isinstance(rx_ext_address, int) or rx_ext_address < 0 or rx_ext_address > 0xFF:
                raise ValueError("rx_ext_address must be a an integer between 0 and FF")
            o.rx_ext_address = rx_ext_address
            o.optflag |= flags.RX_EXT_ADDR

        if tx_stmin is not None:
            if not isinstance(tx_stmin, int) or tx_stmin < 0 or tx_stmin > 0xFFFFFFFF:
                raise ValueError("tx_stmin must be a valid 32 unsigned integer")
            o.optflag |= flags.FORCE_TXSTMIN
            s.setsockopt(SOL_CAN_ISOTP, CAN_ISOTP_TX_STMIN, struct.pack("=L", tx_stmin))
        else:
            # Does not make sense to let the user force STmin value without providing it
            o.optflag &= ~flags.FORCE_TXSTMIN

        opt = struct.pack("=LLBBBB", o.optflag, o.frame_txtime, o.ext_address, o.txpad, o.rxpad, o.rx_ext_address)
        s.setsockopt(SOL_CAN_ISOTP, CAN_ISOTP_OPTS, opt)
        return o

    def __repr__(self) -> str:
        optflag_str = '[undefined]' if self.optflag is None else '0x%08x' % (self.optflag)
        frame_txtime_str = '[undefined]' if self.frame_txtime is None else '0x%08x' % (self.frame_txtime)
        ext_address_str = '[undefined]' if self.ext_address is None else '0x%02x' % (self.ext_address)
        txpad_str = '[undefined]' if self.txpad is None else '0x%02x' % (self.txpad)
        rxpad_str = '[undefined]' if self.rxpad is None else '0x%02x' % (self.rxpad)
        rx_ext_address_str = '[undefined]' if self.rx_ext_address is None else '0x%02x' % (self.rx_ext_address)

        return "<GeneralOpts: optflag=%s, frame_txtime=%s, ext_address=%s, txpad=%s, rxpad=%s, rx_ext_address=%s>" % (optflag_str, frame_txtime_str, ext_address_str, txpad_str, rxpad_str, rx_ext_address_str)


class FlowControlOpts:
    struct_size = 3

    stmin: Optional[int]
    bs: Optional[int]
    wftmax: Optional[int]

    def __init__(self) -> None:
        self.stmin = None
        self.bs = None
        self.wftmax = None

    @classmethod
    def read(cls, s: socket_module.socket) -> "FlowControlOpts":
        assert_is_socket(s)
        o = cls()
        opt = s.getsockopt(SOL_CAN_ISOTP, CAN_ISOTP_RECV_FC, cls.struct_size)

        (o.bs, o.stmin, o.wftmax) = struct.unpack("=BBB", opt)
        return o

    @classmethod
    def write(cls,
              s: socket_module.socket,
              bs: Optional[int] = None,
              stmin: Optional[int] = None,
              wftmax: Optional[int] = None
              ) -> "FlowControlOpts":
        assert_is_socket(s)
        o = cls.read(s)
        if bs != None:
            if not isinstance(bs, int) or bs < 0 or bs > 0xFF:
                raise ValueError("bs (block size) must be a valid integer between 0 and FF")
            o.bs = bs

        if stmin != None:
            if not isinstance(stmin, int) or stmin < 0 or stmin > 0xFF:
                raise ValueError("stmin (separation time) must be a valid integer between 0 and FF")
            o.stmin = stmin

        if wftmax != None:
            if not isinstance(wftmax, int) or wftmax < 0 or wftmax > 0xFF:
                raise ValueError("wftmax (wait frame max) must be a valid integer between 0 and FF")
            o.wftmax = wftmax

        opt = struct.pack("=BBB", o.bs, o.stmin, o.wftmax)
        s.setsockopt(SOL_CAN_ISOTP, CAN_ISOTP_RECV_FC, opt)
        return o

    def __repr__(self) -> str:
        bs_str = '[undefined]' if self.bs is None else '0x%02x' % (self.bs)
        stmin_str = '[undefined]' if self.stmin is None else '0x%02x' % (self.stmin)
        wftmax_str = '[undefined]' if self.wftmax is None else '0x%02x' % (self.wftmax)
        return "<FlowControlOpts: bs=%s, stmin=%s, wftmax=%s>" % (bs_str, stmin_str, wftmax_str)


class LinkLayerOpts:
    struct_size = 3

    mtu: Optional[int]
    tx_dl: Optional[int]
    tx_flags: Optional[int]

    def __init__(self) -> None:
        self.mtu = None
        self.tx_dl = None
        self.tx_flags = None

    @classmethod
    def read(cls, s: socket_module.socket) -> "LinkLayerOpts":
        assert_is_socket(s)
        o = cls()
        opt = s.getsockopt(SOL_CAN_ISOTP, CAN_ISOTP_LL_OPTS, cls.struct_size)

        (o.mtu, o.tx_dl, o.tx_flags) = struct.unpack("=BBB", opt)
        return o

    @classmethod
    def write(cls,
              s: socket_module.socket,
              mtu: Optional[int] = None,
              tx_dl: Optional[int] = None,
              tx_flags: Optional[int] = None
              ) -> "LinkLayerOpts":
        assert_is_socket(s)
        o = cls.read(s)
        if mtu != None:
            if not isinstance(mtu, int) or mtu < 0 or mtu > 0xFF:
                raise ValueError("mtu must be a valid integer between 0 and FF")
            o.mtu = mtu

        if tx_dl != None:
            if not isinstance(tx_dl, int) or tx_dl < 0 or tx_dl > 0xFF:
                raise ValueError("tx_dl must be a valid integer between 0 and FF")
            o.tx_dl = tx_dl

        if tx_flags != None:
            if not isinstance(tx_flags, int) or tx_flags < 0 or tx_flags > 0xFF:
                raise ValueError("tx_flags must be a valid integer between 0 and FF")
            o.tx_flags = tx_flags

        opt = struct.pack("=BBB", o.mtu, o.tx_dl, o.tx_flags)
        s.setsockopt(SOL_CAN_ISOTP, CAN_ISOTP_LL_OPTS, opt)
        return o

    def __repr__(self) -> str:
        mtu_str = '[undefined]' if self.mtu is None else '0x%02x' % (self.mtu)
        tx_dl_str = '[undefined]' if self.tx_dl is None else '0x%02x' % (self.tx_dl)
        tx_flags_str = '[undefined]' if self.tx_flags is None else '0x%02x' % (self.tx_flags)
        return "<LinkLayerOpts: mtu=%s, tx_dl=%s, tx_flags=%s>" % (mtu_str, tx_dl_str, tx_flags_str)


# Backward compatibility with v1.x
general = GeneralOpts
flowcontrol = FlowControlOpts
linklayer = LinkLayerOpts
