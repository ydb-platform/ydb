"""Special Line Character support for Telnet Linemode Option (:rfc:`1184`)."""

from __future__ import annotations

# std imports
from typing import Any, Dict, List, Tuple, Union, Callable, Optional

# local
from .telopt import theNULL
from .accessories import eightbits, name_unicode

__all__ = (
    "BSD_SLC_TAB",
    "Forwardmask",
    "generate_forwardmask",
    "generate_slctab",
    "Linemode",
    "LMODE_FORWARDMASK",
    "LMODE_MODE",
    "LMODE_MODE_REMOTE",
    "LMODE_SLC",
    "name_slc_command",
    "NSLC",
    "SLC",
    "SLC_ABORT",
    "SLC_ACK",
    "SLC_AO",
    "SLC_AYT",
    "SLC_CANTCHANGE",
    "SLC_DEFAULT",
    "SLC_EC",
    "SLC_EL",
    "SLC_EOF",
    "SLC_EW",
    "SLC_IP",
    "SLC_LNEXT",
    "SLC_nosupport",
    "SLC_NOSUPPORT",
    "SLC_RP",
    "SLC_SUSP",
    "SLC_SYNCH",
    "SLC_VARIABLE",
    "SLC_XON",
    "snoop",
    "theNULL",
)

SLC_NOSUPPORT, SLC_CANTCHANGE, SLC_VARIABLE, SLC_DEFAULT = (
    bytes([const]) for const in range(4)
)  # 0, 1, 2, 3
SLC_FLUSHOUT, SLC_FLUSHIN, SLC_ACK = (bytes([2**const]) for const in range(5, 8))  # 32, 64, 128

SLC_LEVELBITS = 0x03
NSLC = 30
(
    SLC_SYNCH,
    SLC_BRK,
    SLC_IP,
    SLC_AO,
    SLC_AYT,
    SLC_EOR,
    SLC_ABORT,
    SLC_EOF,
    SLC_SUSP,
    SLC_EC,
    SLC_EL,
    SLC_EW,
    SLC_RP,
    SLC_LNEXT,
    SLC_XON,
    SLC_XOFF,
    SLC_FORW1,
    SLC_FORW2,
    SLC_MCL,
    SLC_MCR,
    SLC_MCWL,
    SLC_MCWR,
    SLC_MCBOL,
    SLC_MCEOL,
    SLC_INSRT,
    SLC_OVER,
    SLC_ECR,
    SLC_EWR,
    SLC_EBOL,
    SLC_EEOL,
) = (bytes([const]) for const in range(1, NSLC + 1))

LMODE_MODE, LMODE_FORWARDMASK, LMODE_SLC = (bytes([const]) for const in range(1, 4))
LMODE_MODE_REMOTE, LMODE_MODE_LOCAL, LMODE_MODE_TRAPSIG = (bytes([const]) for const in range(3))
LMODE_MODE_ACK, LMODE_MODE_SOFT_TAB, LMODE_MODE_LIT_ECHO = (bytes([4]), bytes([8]), bytes([16]))


class SLC:
    """Defines the willingness to support a Special Linemode Character."""

    def __init__(self, mask: bytes = SLC_DEFAULT, value: bytes = theNULL) -> None:
        """
        Initialize SLC with the given mask and value.

        Defined by its SLC support level, ``mask`` and default keyboard
        ASCII byte ``value`` (may be negotiated by client).
        """
        #   The default byte mask ``SLC_DEFAULT`` and value ``b'\x00'`` infer
        #   our willingness to support the option, but with no default value.
        #   The value must be negotiated by client to activate the callback.
        self.mask = mask
        self.val = value

    @property
    def level(self) -> bytes:
        """Returns SLC level of support."""
        return bytes([ord(self.mask) & SLC_LEVELBITS])

    @property
    def nosupport(self) -> bool:
        """Returns True if SLC level is SLC_NOSUPPORT."""
        return self.level == SLC_NOSUPPORT

    @property
    def cantchange(self) -> bool:
        """Returns True if SLC level is SLC_CANTCHANGE."""
        return self.level == SLC_CANTCHANGE

    @property
    def variable(self) -> bool:
        """Returns True if SLC level is SLC_VARIABLE."""
        return self.level == SLC_VARIABLE

    @property
    def default(self) -> bool:
        """Returns True if SLC level is SLC_DEFAULT."""
        return self.level == SLC_DEFAULT

    @property
    def ack(self) -> int:
        """Returns True if SLC_ACK bit is set."""
        return ord(self.mask) & ord(SLC_ACK)

    @property
    def flushin(self) -> int:
        """Returns True if SLC_FLUSHIN bit is set."""
        return ord(self.mask) & ord(SLC_FLUSHIN)

    @property
    def flushout(self) -> int:
        """Returns True if SLC_FLUSHOUT bit is set."""
        return ord(self.mask) & ord(SLC_FLUSHOUT)

    def set_value(self, value: bytes) -> None:
        """Set SLC keyboard ascii value to ``byte``."""
        self.val = value

    def set_mask(self, mask: bytes) -> None:
        """Set SLC option mask, ``mask``."""
        self.mask = mask

    def set_flag(self, flag: bytes) -> None:
        """Set SLC option flag, ``flag``."""
        self.mask = bytes([ord(self.mask) | ord(flag)])

    def __str__(self) -> str:
        """SLC definition as string '(value, flag(|s))'."""
        flags = []
        for flag in (
            "nosupport",
            "variable",
            "default",
            "ack",
            "flushin",
            "flushout",
            "cantchange",
        ):
            if getattr(self, flag):
                flags.append(flag)
        value_str = (
            name_unicode(chr(self.val[0])) if self.val != _POSIX_VDISABLE else "(DISABLED:\\xff)"
        )
        return f"({value_str}, {'|'.join(flags)})"


class SLC_nosupport(SLC):
    """SLC definition inferring our unwillingness to support the option."""

    def __init__(self) -> None:
        """Initialize SLC_nosupport with NOSUPPORT level and disabled value."""
        SLC.__init__(self, SLC_NOSUPPORT, _POSIX_VDISABLE)


#: SLC value may be changed, flushes input and output
_SLC_VARIABLE_FIO = bytes([ord(SLC_VARIABLE) | ord(SLC_FLUSHIN) | ord(SLC_FLUSHOUT)])
#: SLC value may be changed, flushes input
_SLC_VARIABLE_FI = bytes([ord(SLC_VARIABLE) | ord(SLC_FLUSHIN)])
#: SLC value may be changed, flushes output
_SLC_VARIABLE_FO = bytes([ord(SLC_VARIABLE) | ord(SLC_FLUSHOUT)])
#: SLC function for this value is not supported
_POSIX_VDISABLE = b"\xff"

#: This SLC tab when sent to a BSD client warrants no reply; their
#  tabs match exactly. These values are found in ttydefaults.h of
#  termios family of functions.
BSD_SLC_TAB = {
    SLC_FORW1: SLC_nosupport(),  # unsupported; causes all buffered
    SLC_FORW2: SLC_nosupport(),  # characters to be sent immediately,
    SLC_EOF: SLC(SLC_VARIABLE, b"\x04"),  # ^D VEOF
    SLC_EC: SLC(SLC_VARIABLE, b"\x7f"),  # BS VERASE
    SLC_EL: SLC(SLC_VARIABLE, b"\x15"),  # ^U VKILL
    SLC_IP: SLC(_SLC_VARIABLE_FIO, b"\x03"),  # ^C VINTR
    SLC_ABORT: SLC(_SLC_VARIABLE_FIO, b"\x1c"),  # ^\ VQUIT
    SLC_XON: SLC(SLC_VARIABLE, b"\x11"),  # ^Q VSTART
    SLC_XOFF: SLC(SLC_VARIABLE, b"\x13"),  # ^S VSTOP
    SLC_EW: SLC(SLC_VARIABLE, b"\x17"),  # ^W VWERASE
    SLC_RP: SLC(SLC_VARIABLE, b"\x12"),  # ^R VREPRINT
    SLC_LNEXT: SLC(SLC_VARIABLE, b"\x16"),  # ^V VLNEXT
    SLC_AO: SLC(_SLC_VARIABLE_FO, b"\x0f"),  # ^O VDISCARD
    SLC_SUSP: SLC(_SLC_VARIABLE_FI, b"\x1a"),  # ^Z VSUSP
    SLC_AYT: SLC(SLC_VARIABLE, b"\x14"),  # ^T VSTATUS
    # no default value for break, sync, end-of-record,
    SLC_BRK: SLC(),
    SLC_SYNCH: SLC(),
    SLC_EOR: SLC(),
}


def generate_slctab(tabset: Optional[Dict[bytes, SLC]] = None) -> Dict[bytes, SLC]:
    """
    Returns full 'SLC Tab' for definitions found using ``tabset``.

    Functions not listed in ``tabset`` are set as SLC_NOSUPPORT.
    """
    if tabset is None:
        tabset = BSD_SLC_TAB
    #   ``slctab`` is a dictionary of SLC functions, such as SLC_IP,
    #   to a tuple of the handling character and support level.
    _slctab = {}
    for slc in [bytes([const]) for const in range(1, NSLC + 1)]:
        _slctab[slc] = tabset.get(slc, SLC_nosupport())
    return _slctab


def generate_forwardmask(
    binary_mode: bool, tabset: Dict[bytes, SLC], ack: bool = False
) -> "Forwardmask":
    """
    Generate a Forwardmask instance.

    Generate a 32-byte (``binary_mode`` is True) or 16-byte (False) Forwardmask
    instance appropriate for the specified ``slctab``.  A Forwardmask is formed
    by a bitmask of all 256 possible 8-bit keyboard ascii input, or, when not
    'outbinary', a 16-byte 7-bit representation of each value, and whether
    they should be "forwarded" by the client on the transport stream
    """
    num_bytes, msb = (32, 256) if binary_mode else (16, 127)
    mask32 = [theNULL] * num_bytes
    for mask in range(msb // 8):
        start = mask * 8
        last = start + 7
        byte = theNULL
        for char in range(start, last + 1):
            func, _, slc_def = snoop(bytes([char]), tabset, {})
            if func is not None and slc_def is not None and not slc_def.nosupport:
                # set bit for this character, it is a supported slc char
                byte = bytes([ord(byte) | 1])
            if char != last:
                # shift byte left for next character,
                # except for the final byte.
                byte = bytes([ord(byte) << 1])
        mask32[mask] = byte
    return Forwardmask(b"".join(mask32), ack)


def snoop(
    byte: bytes, slctab: Dict[bytes, SLC], slc_callbacks: Dict[bytes, Callable[..., Any]]
) -> Tuple[Optional[Callable[..., Any]], Optional[bytes], Optional[SLC]]:
    """
    Scan ``slctab`` for matching ``byte`` values.

    Returns (callback, func_byte, slc_definition) on match. Otherwise, (None, None, None). If no
    callback is assigned, the value of callback is always None.
    """
    for slc_func, slc_def in slctab.items():
        if byte == slc_def.val and slc_def.val != theNULL:
            return (slc_callbacks.get(slc_func, None), slc_func, slc_def)
    return (None, None, None)


class Linemode:
    r"""
    Represents the LINEMODE negotiation state.

    A mask of ``LMODE_MODE_LOCAL`` means that all line editing is performed
    on the client side (default). A mask of theNULL (``\x00``) indicates
    that editing is performed on the remote side.
    """

    def __init__(self, mask: bytes = b"\x00") -> None:
        """
        Initialize Linemode with the given mask.

        Valid bit flags of mask are: ``LMODE_MODE_TRAPSIG``, ``LMODE_MODE_ACK``,
        ``LMODE_MODE_SOFT_TAB``, and ``LMODE_MODE_LIT_ECHO``.
        """
        self.mask = mask

    def __eq__(self, other: object) -> bool:
        """Compare by another Linemode (LMODE_MODE_ACK ignored)."""
        # the inverse OR(|) of acknowledge bit UNSET in comparator,
        # would be the AND OR(& ~) to compare modes without acknowledge
        # bit set.
        if not isinstance(other, Linemode):
            return NotImplemented
        return (ord(self.mask) | ord(LMODE_MODE_ACK)) == (ord(other.mask) | ord(LMODE_MODE_ACK))

    @property
    def local(self) -> bool:
        """True if linemode is local."""
        return bool(ord(self.mask) & ord(LMODE_MODE_LOCAL))

    @property
    def remote(self) -> bool:
        """True if linemode is remote."""
        return not self.local

    @property
    def trapsig(self) -> bool:
        """True if signals are trapped by client."""
        return bool(ord(self.mask) & ord(LMODE_MODE_TRAPSIG))

    @property
    def ack(self) -> bool:
        """Returns True if mode has been acknowledged."""
        return bool(ord(self.mask) & ord(LMODE_MODE_ACK))

    @property
    def soft_tab(self) -> bool:
        r"""Returns True if client will expand horizontal tab (``\x09``)."""
        return bool(ord(self.mask) & ord(LMODE_MODE_SOFT_TAB))

    @property
    def lit_echo(self) -> bool:
        """Returns True if non-printable characters are displayed as-is."""
        return bool(ord(self.mask) & ord(LMODE_MODE_LIT_ECHO))

    def __str__(self) -> str:
        """Returns string representation of line mode, for debugging."""
        return "remote" if self.remote else "local"

    def __repr__(self) -> str:
        props = ", ".join(
            f"{prop}:{getattr(self, prop)}"
            for prop in ("lit_echo", "soft_tab", "ack", "trapsig", "remote", "local")
        )
        return f"<{self.mask!r}: {props}>"


class Forwardmask:
    """Forwardmask object using the bytemask value received by server."""

    def __init__(self, value: Union[bytes, bytearray], ack: bool = False) -> None:
        """
        Initialize Forwardmask with the given value.

        :param value: Bytemask ``value`` received by server after ``IAC SB
            LINEMODE DO FORWARDMASK``. It must be a bytearray of length 16 or 32.
        """
        self.value = value
        self.ack = ack

    def description_table(self) -> List[str]:
        """Returns list of strings describing obj as a tabular ASCII map."""
        result: List[str] = []
        mrk_cont = "(...)"

        def continuing() -> bool:
            return bool(result and result[-1] == mrk_cont)

        def is_last(mask: int) -> bool:
            return mask == len(self.value) - 1

        def same_as_last(row: str) -> bool:
            return bool(result and result[-1].endswith(row.split()[-1]))

        for mask, byte in enumerate(self.value):
            if byte == 0:
                if continuing() and not is_last(mask):
                    continue
                row = f"[{mask:2d}] {eightbits(0)}"
                if not same_as_last(row) or is_last(mask):
                    result.append(row)
                else:
                    result.append(mrk_cont)
            else:
                start = mask * 8
                last = start + 7
                characters = ", ".join(
                    [name_unicode(chr(char)) for char in range(start, last + 1) if char in self]
                )
                result.append(f"[{mask:2d}] {eightbits(byte)} {characters}")
        return result

    def __str__(self) -> str:
        """Returns single string of binary 0 and 1 describing obj."""
        bits = "".join(value for (_, value) in [eightbits(byte).split("b") for byte in self.value])
        return f"0b{bits}"

    def __contains__(self, number: int) -> bool:
        """Whether forwardmask contains keycode ``number``."""
        mask, flag = number // 8, 2 ** (7 - (number % 8))
        return bool(self.value[mask] & flag)


#: List of globals that may match an slc function byte
_DEBUG_SLC_OPTS = {
    value: key
    for key, value in locals().items()
    if key
    in (
        "SLC_SYNCH",
        "SLC_BRK",
        "SLC_IP",
        "SLC_AO",
        "SLC_AYT",
        "SLC_EOR",
        "SLC_ABORT",
        "SLC_EOF",
        "SLC_SUSP",
        "SLC_EC",
        "SLC_EL",
        "SLC_EW",
        "SLC_RP",
        "SLC_LNEXT",
        "SLC_XON",
        "SLC_XOFF",
        "SLC_FORW1",
        "SLC_FORW2",
        "SLC_MCL",
        "SLC_MCR",
        "SLC_MCWL",
        "SLC_MCWR",
        "SLC_MCBOL",
        "SLC_MCEOL",
        "SLC_INSRT",
        "SLC_OVER",
        "SLC_ECR",
        "SLC_EWR",
        "SLC_EBOL",
        "SLC_EEOL",
    )
}


def name_slc_command(byte: bytes) -> str:
    """Given an SLC ``byte``, return global mnemonic as string."""
    return repr(byte) if byte not in _DEBUG_SLC_OPTS else _DEBUG_SLC_OPTS[byte]
