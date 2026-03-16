"""Telnet option constants exported from the deprecated telnetlib module."""

# std imports
from typing import Dict

# Exported from the telnetlib module, which is marked for deprecation in version
# 3.11 and removal in 3.13
LINEMODE = b'"'
NAWS = b"\x1f"
NEW_ENVIRON = b"'"
BINARY = b"\x00"
SGA = b"\x03"
ECHO = b"\x01"
STATUS = b"\x05"
TTYPE = b"\x18"
TSPEED = b" "
LFLOW = b"!"
XDISPLOC = b"#"
IAC = b"\xff"
DONT = b"\xfe"
DO = b"\xfd"
WONT = b"\xfc"
WILL = b"\xfb"
SE = b"\xf0"
NOP = b"\xf1"
TM = b"\x06"
DM = b"\xf2"
BRK = b"\xf3"
IP = b"\xf4"
AO = b"\xf5"
AYT = b"\xf6"
EC = b"\xf7"
EL = b"\xf8"
EOR = b"\x19"
GA = b"\xf9"
SB = b"\xfa"
LOGOUT = b"\x12"
CHARSET = b"*"
SNDLOC = b"\x17"
theNULL = b"\x00"
ENCRYPT = b"&"
AUTHENTICATION = b"%"
TN3270E = b"("
XAUTH = b")"
RSP = b"+"
COM_PORT_OPTION = b","
SUPPRESS_LOCAL_ECHO = b"-"
TLS = b"."
KERMIT = b"/"
SEND_URL = b"0"
FORWARD_X = b"1"
PRAGMA_LOGON = b"\x8a"
SSPI_LOGON = b"\x8b"
PRAGMA_HEARTBEAT = b"\x8c"
EXOPL = b"\xff"
X3PAD = b"\x1e"
VT3270REGIME = b"\x1d"
TTYLOC = b"\x1c"
SUPDUPOUTPUT = b"\x16"
SUPDUP = b"\x15"
DET = b"\x14"
BM = b"\x13"
XASCII = b"\x11"
RCP = b"\x02"
NAMS = b"\x04"
RCTE = b"\x07"
NAOL = b"\x08"
NAOP = b"\t"
NAOCRD = b"\n"
NAOHTS = b"\x0b"
NAOHTD = b"\x0c"
NAOFFD = b"\r"
NAOVTS = b"\x0e"
NAOVTD = b"\x0f"
NAOLFD = b"\x10"

__all__ = (
    "AARDWOLF",
    "ABORT",
    "ACCEPTED",
    "AO",
    "ATCP",
    "AUTHENTICATION",
    "AYT",
    "BINARY",
    "BM",
    "BRK",
    "CHARSET",
    "CMD_EOR",
    "COM_PORT_OPTION",
    "DET",
    "DM",
    "DO",
    "DONT",
    "EC",
    "ECHO",
    "EL",
    "ENCRYPT",
    "EOF",
    "EOR",
    "ESC",
    "EXOPL",
    "FORWARD_X",
    "GA",
    "GMCP",
    "IAC",
    "INFO",
    "IP",
    "IS",
    "KERMIT",
    "LFLOW",
    "LFLOW_OFF",
    "LFLOW_ON",
    "LFLOW_RESTART_ANY",
    "LFLOW_RESTART_XON",
    "LINEMODE",
    "LOGOUT",
    "MCCP2_COMPRESS",
    "MCCP_COMPRESS",
    "MSDP",
    "MSDP_ARRAY_CLOSE",
    "MSDP_ARRAY_OPEN",
    "MSDP_TABLE_CLOSE",
    "MSDP_TABLE_OPEN",
    "MSDP_VAL",
    "MSDP_VAR",
    "MSP",
    "MSSP",
    "MSSP_VAL",
    "MSSP_VAR",
    "MXP",
    "NAMS",
    "NAOCRD",
    "NAOFFD",
    "NAOHTD",
    "NAOHTS",
    "NAOL",
    "NAOLFD",
    "NAOP",
    "NAOVTD",
    "NAOVTS",
    "NAWS",
    "NEW_ENVIRON",
    "NOP",
    "PRAGMA_HEARTBEAT",
    "PRAGMA_LOGON",
    "RCP",
    "RCTE",
    "REJECTED",
    "REQUEST",
    "RSP",
    "SB",
    "SE",
    "SEND",
    "SEND_URL",
    "SGA",
    "SNDLOC",
    "SSPI_LOGON",
    "STATUS",
    "SUPDUP",
    "SUPDUPOUTPUT",
    "TELOPT_92",
    "SUPPRESS_LOCAL_ECHO",
    "SUSP",
    "TLS",
    "TM",
    "TN3270E",
    "TSPEED",
    "TTABLE_ACK",
    "TTABLE_IS",
    "TTABLE_NAK",
    "TTABLE_REJECTED",
    "TTYLOC",
    "TTYPE",
    "USERVAR",
    "VALUE",
    "VAR",
    "VT3270REGIME",
    "WILL",
    "WONT",
    "X3PAD",
    "XASCII",
    "XAUTH",
    "XDISPLOC",
    "ZMP",
    "theNULL",
    "name_command",
    "name_commands",
    "name_option",
    "option_from_name",
)

EOF, SUSP, ABORT, CMD_EOR = (bytes([const]) for const in range(236, 240))
IS, SEND, INFO = (bytes([const]) for const in range(3))
VAR, VALUE, ESC, USERVAR = (bytes([const]) for const in range(4))
LFLOW_OFF, LFLOW_ON, LFLOW_RESTART_ANY, LFLOW_RESTART_XON = (bytes([const]) for const in range(4))
REQUEST, ACCEPTED, REJECTED, TTABLE_IS, TTABLE_REJECTED, TTABLE_ACK, TTABLE_NAK = (
    bytes([const]) for const in range(1, 8)
)
MCCP_COMPRESS, MCCP2_COMPRESS = (bytes([85]), bytes([86]))
GMCP = bytes([201])
MSDP = bytes([69])
MSSP = bytes([70])
MSP = bytes([90])
MXP = bytes([91])
TELOPT_92 = bytes([92])
ZMP = bytes([93])
AARDWOLF = bytes([102])
ATCP = bytes([200])

# MSDP sub-command bytes (used within SB MSDP payloads)
MSDP_VAR = bytes([1])
MSDP_VAL = bytes([2])
MSDP_TABLE_OPEN = bytes([3])
MSDP_TABLE_CLOSE = bytes([4])
MSDP_ARRAY_OPEN = bytes([5])
MSDP_ARRAY_CLOSE = bytes([6])

# MSSP sub-command bytes (used within SB MSSP payloads)
MSSP_VAR = bytes([1])
MSSP_VAL = bytes([2])

#: List of globals that may match an iac command option bytes
_DEBUG_OPTS: Dict[bytes, str] = {
    value: key
    for key, value in globals().items()
    if key
    in (
        "LINEMODE",
        "LMODE_FORWARDMASK",
        "NAWS",
        "NEW_ENVIRON",
        "ENCRYPT",
        "AUTHENTICATION",
        "BINARY",
        "SGA",
        "ECHO",
        "STATUS",
        "TTYPE",
        "TSPEED",
        "LFLOW",
        "XDISPLOC",
        "IAC",
        "DONT",
        "DO",
        "WONT",
        "WILL",
        "SE",
        "NOP",
        "DM",
        "TM",
        "BRK",
        "IP",
        "ABORT",
        "AO",
        "AYT",
        "EC",
        "EL",
        "EOR",
        "GA",
        "SB",
        "EOF",
        "SUSP",
        "ABORT",
        "CMD_EOR",
        "LOGOUT",
        "CHARSET",
        "SNDLOC",
        "MCCP_COMPRESS",
        "MCCP2_COMPRESS",
        "GMCP",
        "MSDP",
        "MSSP",
        "MSP",
        "MXP",
        "TELOPT_92",
        "ZMP",
        "AARDWOLF",
        "ATCP",
        "ENCRYPT",
        "AUTHENTICATION",
        "TN3270E",
        "XAUTH",
        "RSP",
        "COM_PORT_OPTION",
        "SUPPRESS_LOCAL_ECHO",
        "TLS",
        "KERMIT",
        "SEND_URL",
        "FORWARD_X",
        "PRAGMA_LOGON",
        "SSPI_LOGON",
        "PRAGMA_HEARTBEAT",
        "EXOPL",
        "X3PAD",
        "VT3270REGIME",
        "TTYLOC",
        "SUPDUPOUTPUT",
        "SUPDUP",
        "DET",
        "BM",
        "XASCII",
        "RCP",
        "NAMS",
        "RCTE",
        "NAOL",
        "NAOP",
        "NAOCRD",
        "NAOHTS",
        "NAOHTD",
        "NAOFFD",
        "NAOVTS",
        "NAOVTD",
        "NAOLFD",
    )
}

#: Reverse mapping of option names to option bytes
_NAME_TO_OPT: Dict[str, bytes] = {name: opt for opt, name in _DEBUG_OPTS.items()}


def option_from_name(name: str) -> bytes:
    """
    Return option bytes for a given option name.

    :param name: Option name (e.g., "NAWS", "TTYPE")
    :returns: Option bytes
    :raises KeyError: If name is not a known telnet option
    """
    return _NAME_TO_OPT[name.upper()]


def name_command(byte: bytes) -> str:
    """Return string description for (maybe) telnet command byte."""
    return _DEBUG_OPTS.get(byte, repr(byte))


#: IAC command bytes that should display as hex when used as option codes.
#: Servers with output-filter bugs can send e.g. ``IAC WONT 0xFC`` where
#: 0xFC is the WONT command byte itself.  Displaying "WONT WONT" is
#: confusing, so :func:`name_option` renders these as ``b'\\xfc'``.
_IAC_CMD_BYTES: frozenset[bytes] = frozenset(
    {IAC, DO, DONT, WILL, WONT, SB, SE, NOP, DM, BRK, IP, AO, AYT, EC, EL, GA}
)


def name_option(byte: bytes) -> str:
    """
    Return string description for a telnet option byte.

    Unlike :func:`name_command`, IAC command bytes (DO, DONT, WILL, WONT,
    etc.) are displayed as ``repr(byte)`` rather than their command names
    when they appear in the option-byte position.
    """
    if byte in _IAC_CMD_BYTES:
        return repr(byte)
    return _DEBUG_OPTS.get(byte, repr(byte))


def name_commands(cmds: bytes, sep: str = " ") -> str:
    """Return string description for array of (maybe) telnet command bytes."""
    return sep.join([name_command(bytes([byte])) for byte in cmds])
