"""
Fingerprint shell for telnet client identification.

This module runs **server-side**: it is the shell callback for a telnetlib3
server that probes connecting *clients* for protocol capabilities, collects
session data, and saves fingerprint files.  Despite the generic name, it
fingerprints the remote *client*, not the server.

Display, REPL, and post-script code live in
``telnetlib3.fingerprinting_display``.
"""

from __future__ import annotations

# std imports
import os
import sys
import json
import time
import asyncio
import hashlib
import logging
import argparse
import datetime
from typing import Any, Union, Optional, TypedDict, cast

# local
from . import slc
from ._paths import _atomic_json_write
from .server import TelnetServer
from .telopt import (
    BM,
    DO,
    DET,
    EOR,
    MSP,
    MXP,
    RCP,
    RSP,
    SGA,
    TLS,
    ZMP,
    ATCP,
    DONT,
    ECHO,
    GMCP,
    MSDP,
    MSSP,
    NAMS,
    NAOL,
    NAOP,
    NAWS,
    RCTE,
    LFLOW,
    TTYPE,
    X3PAD,
    XAUTH,
    BINARY,
    KERMIT,
    NAOCRD,
    NAOFFD,
    NAOHTD,
    NAOHTS,
    NAOLFD,
    NAOVTD,
    NAOVTS,
    SNDLOC,
    STATUS,
    SUPDUP,
    TSPEED,
    TTYLOC,
    CHARSET,
    ENCRYPT,
    TN3270E,
    AARDWOLF,
    LINEMODE,
    SEND_URL,
    XDISPLOC,
    FORWARD_X,
    SSPI_LOGON,
    NEW_ENVIRON,
    PRAGMA_LOGON,
    SUPDUPOUTPUT,
    VT3270REGIME,
    AUTHENTICATION,
    COM_PORT_OPTION,
    PRAGMA_HEARTBEAT,
    SUPPRESS_LOCAL_ECHO,
    theNULL,
)
from .accessories import encoding_from_lang
from .stream_reader import TelnetReader, TelnetReaderUnicode
from .stream_writer import TelnetWriter, TelnetWriterUnicode


class ProbeResult(TypedDict, total=False):
    """Result of probing a single telnet option."""

    status: str
    opt: bytes
    description: str
    already_negotiated: bool


# Data directory for saving fingerprint data - None when unset (no saves)
DATA_DIR: Optional[str] = (
    os.environ["TELNETLIB3_DATA_DIR"] if os.environ.get("TELNETLIB3_DATA_DIR") else None
)

# Maximum files per protocol-fingerprint folder
FINGERPRINT_MAX_FILES = int(os.environ.get("TELNETLIB3_FINGERPRINT_MAX_FILES", "1000"))

# Maximum number of unique fingerprint folders
FINGERPRINT_MAX_FINGERPRINTS = int(
    os.environ.get("TELNETLIB3_FINGERPRINT_MAX_FINGERPRINTS", "1000")
)

# Post-fingerprint Python module to execute with saved file path
# Example: TELNETLIB3_FINGERPRINT_POST_SCRIPT=telnetlib3.fingerprinting_display
FINGERPRINT_POST_SCRIPT = os.environ.get("TELNETLIB3_FINGERPRINT_POST_SCRIPT", "")


# Terminal types that uniquely identify specific telnet clients
PROTOCOL_MATCHED_TERMINALS = {"syncterm"}  # SyncTERM BBS client

# Terminal types associated with MUD clients, matched case-insensitively.
# These clients are likely to support extended options like GMCP.
MUD_TERMINALS = {
    "mudlet",
    "cmud",
    "zmud",
    "mushclient",
    "atlantis",
    "tintin++",
    "tt++",
    "blowtorch",
    "mudrammer",
    "kildclient",
    "portal",
    "beip",
    "savitar",
}

__all__ = (
    "ENVIRON_EXTENDED",
    "FingerprintingServer",
    "FingerprintingTelnetServer",
    "ProbeResult",
    "fingerprint_server_main",
    "fingerprinting_server_shell",
    "fingerprinting_post_script",
    "get_client_fingerprint",
    "probe_client_capabilities",
)

#: Extended NEW_ENVIRON variable list used during client fingerprinting.
#: The base :class:`~telnetlib3.server.TelnetServer` requests only common
#: variables (USER, LOGNAME, LANG, TERM, etc.).  This extended set collects
#: additional information useful for identifying and classifying clients.
ENVIRON_EXTENDED: list[str] = [
    "HOME",
    "SHELL",
    "SSH_CLIENT",
    "SSH_TTY",
    "HOSTNAME",
    "HOSTTYPE",
    "OSTYPE",
    "PWD",
    "VISUAL",
    "TMUX",
    "STY",
    "LC_ALL",
    "LC_CTYPE",
    "LC_MESSAGES",
    "LC_COLLATE",
    "LC_TIME",
    "DOCKER_HOST",
    "HISTFILE",
    "AWS_PROFILE",
    "AWS_REGION",
]

logger = logging.getLogger("telnetlib3.fingerprint")


class FingerprintingTelnetServer:
    """
    Mixin that extends ``on_request_environ`` with :data:`ENVIRON_EXTENDED`.

    Usage with :func:`~telnetlib3.server.create_server`::

        from telnetlib3.server import TelnetServer
        from telnetlib3.fingerprinting import FingerprintingTelnetServer

        class MyServer(FingerprintingTelnetServer, TelnetServer):
            pass

        server = await create_server(protocol_factory=MyServer, ...)
    """

    def on_request_environ(self) -> list[Union[str, bytes]]:
        """Return base environ keys plus :data:`ENVIRON_EXTENDED`."""
        if not isinstance(self, TelnetServer):
            raise TypeError("FingerprintingTelnetServer must be combined with TelnetServer")
        # pylint: disable=no-member
        base: list[Union[str, bytes]] = super().on_request_environ()  # type: ignore[misc]
        # Insert extended keys before the trailing VAR/USERVAR sentinels
        from .telopt import VAR, USERVAR

        extra = [k for k in ENVIRON_EXTENDED if k not in base]
        # Find where VAR/USERVAR sentinels start and insert before them
        insert_at = len(base)
        for i, item in enumerate(base):
            if item in (VAR, USERVAR):
                insert_at = i
                break
        return base[:insert_at] + extra + base[insert_at:]


class FingerprintingServer(FingerprintingTelnetServer, TelnetServer):
    """
    :class:`~telnetlib3.server.TelnetServer` with extended ``NEW_ENVIRON``.

    Combines :class:`FingerprintingTelnetServer` with :class:`~telnetlib3.server.TelnetServer`
    so that :func:`fingerprinting_server_shell` receives the full set of
    environment variables needed for stable fingerprint hashes.

    Used as the default ``protocol_factory`` by
    :func:`fingerprint_server_main` / ``telnetlib3-fingerprint-server`` CLI.
    """


# Timeout for probe_client_capabilities in _run_probe (seconds)
_PROBE_TIMEOUT = 0.5

# Telnet options to probe, grouped by category
# Each entry is (option_bytes, name, description)
CORE_OPTIONS = [
    (BINARY, "BINARY", "8-bit binary mode"),
    (SGA, "SGA", "Suppress Go Ahead"),
    (ECHO, "ECHO", "Echo mode"),
    (STATUS, "STATUS", "Option status reporting"),
    (TTYPE, "TTYPE", "Terminal type"),
    (TSPEED, "TSPEED", "Terminal speed"),
    (LFLOW, "LFLOW", "Local flow control"),
    (XDISPLOC, "XDISPLOC", "X display location"),
    (NAWS, "NAWS", "Window size"),
    (NEW_ENVIRON, "NEW_ENVIRON", "Environment variables"),
    (CHARSET, "CHARSET", "Character set"),
    (LINEMODE, "LINEMODE", "Line mode with SLC"),
    (EOR, "EOR", "End of Record"),
    # LOGOUT omitted - BSD client times out on this
    (SNDLOC, "SNDLOC", "Send location"),
]

MUD_OPTIONS = [(COM_PORT_OPTION, "COM_PORT", "Serial port control (RFC 2217)")]

# Options with non-standard byte values (> 140) that crash some clients.
# icy_term (icy_net) only accepts option bytes 0-49, 138-140, and 255,
# returning a hard error for anything else. GMCP-capable MUD clients
# typically self-announce via IAC WILL GMCP, so probing is unnecessary.
EXTENDED_OPTIONS = [
    (GMCP, "GMCP", "Generic MUD Communication Protocol"),
    (MSDP, "MSDP", "MUD Server Data Protocol"),
    (MSSP, "MSSP", "MUD Server Status Protocol"),
    (MSP, "MSP", "MUD Sound Protocol"),
    (MXP, "MXP", "MUD eXtension Protocol"),
    (ZMP, "ZMP", "Zenith MUD Protocol"),
    (AARDWOLF, "AARDWOLF", "Aardwolf protocol"),
    (ATCP, "ATCP", "Achaea Telnet Client Protocol"),
]

LEGACY_OPTIONS = [
    (AUTHENTICATION, "AUTHENTICATION", "Telnet authentication"),
    (ENCRYPT, "ENCRYPT", "Encryption option"),
    (TN3270E, "TN3270E", "3270 terminal emulation"),
    (XAUTH, "XAUTH", "X authentication"),
    (RSP, "RSP", "Remote serial port"),
    (SUPPRESS_LOCAL_ECHO, "SUPPRESS_LOCAL_ECHO", "Local echo suppression"),
    (TLS, "TLS", "TLS negotiation"),
    (KERMIT, "KERMIT", "Kermit file transfer"),
    (SEND_URL, "SEND_URL", "URL sending"),
    (FORWARD_X, "FORWARD_X", "X11 forwarding"),
    (PRAGMA_LOGON, "PRAGMA_LOGON", "Pragma logon"),
    (SSPI_LOGON, "SSPI_LOGON", "SSPI logon"),
    (PRAGMA_HEARTBEAT, "PRAGMA_HEARTBEAT", "Heartbeat"),
    (X3PAD, "X3PAD", "X.3 PAD"),
    (VT3270REGIME, "VT3270REGIME", "VT3270 regime"),
    (TTYLOC, "TTYLOC", "Terminal location"),
    (SUPDUP, "SUPDUP", "SUPDUP protocol"),
    (SUPDUPOUTPUT, "SUPDUPOUTPUT", "SUPDUP output"),
    (DET, "DET", "Data entry terminal"),
    (BM, "BM", "Byte macro"),
    (RCP, "RCP", "Reconnection"),
    (NAMS, "NAMS", "NAMS"),
    (RCTE, "RCTE", "Remote controlled transmit/echo"),
    (NAOL, "NAOL", "Output line width"),
    (NAOP, "NAOP", "Output page size"),
    (NAOCRD, "NAOCRD", "Output CR disposition"),
    (NAOHTS, "NAOHTS", "Output horiz tab stops"),
    (NAOHTD, "NAOHTD", "Output horiz tab disposition"),
    (NAOFFD, "NAOFFD", "Output formfeed disposition"),
    (NAOVTS, "NAOVTS", "Output vert tabstops"),
    (NAOVTD, "NAOVTD", "Output vert tab disposition"),
    (NAOLFD, "NAOLFD", "Output LF disposition"),
]

ALL_PROBE_OPTIONS = CORE_OPTIONS + MUD_OPTIONS + LEGACY_OPTIONS
QUICK_PROBE_OPTIONS = CORE_OPTIONS + MUD_OPTIONS

# All known options including extended, for display/name lookup only
_ALL_KNOWN_OPTIONS = ALL_PROBE_OPTIONS + EXTENDED_OPTIONS

# Build mapping from hex string (e.g., "0x03") to option name (e.g., "SGA")
_OPT_BYTE_TO_NAME = {f"0x{opt[0]:02x}": name for opt, name, _ in _ALL_KNOWN_OPTIONS}


async def probe_client_capabilities(
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    options: Optional[list[tuple[bytes, str, str]]] = None,
    timeout: float = 0.5,
) -> dict[str, ProbeResult]:
    """
    Actively probe client for telnet capability support.

    Sends IAC DO for ALL options at once, waits for responses, then collects results.

    :param writer: TelnetWriter instance.
    :param options: List of (opt_bytes, name, description) tuples to probe. Defaults to
        ALL_PROBE_OPTIONS.
    :param timeout: Timeout in seconds to wait for all responses.
    :returns: Dict mapping option name to :class:`ProbeResult`.
    """
    if options is None:
        options = ALL_PROBE_OPTIONS

    results: dict[str, ProbeResult] = {}
    to_probe = []

    for opt, name, description in options:
        if writer.remote_option.enabled(opt):
            results[name] = ProbeResult(
                status="WILL", opt=opt, description=description, already_negotiated=True
            )
        elif writer.remote_option.get(opt) is False:
            results[name] = ProbeResult(
                status="WONT", opt=opt, description=description, already_negotiated=True
            )
        else:
            to_probe.append((opt, name, description))

    for opt, name, description in to_probe:
        writer.iac(DO, opt)

    await writer.drain()

    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        all_responded = all(
            writer.remote_option.get(opt) is not None
            for opt, name, desc in to_probe
            if name not in results
        )
        if all_responded:
            break
        await asyncio.sleep(0.05)

    for opt, name, description in to_probe:
        if name in results:
            continue

        if writer.remote_option.enabled(opt):
            results[name] = ProbeResult(status="WILL", opt=opt, description=description)
        elif writer.remote_option.get(opt) is False:
            results[name] = ProbeResult(status="WONT", opt=opt, description=description)
        else:
            results[name] = ProbeResult(status="timeout", opt=opt, description=description)

    return results


# Keys to collect from extra_info
_EXTRA_INFO_KEYS = (
    "TERM",
    "term",
    "cols",
    "rows",
    "COLUMNS",
    "LINES",
    "charset",
    "LANG",
    "COLORTERM",
    "peername",
    "sockname",
    "tspeed",
    "xdisploc",
    "DISPLAY",
    "encoding",
) + tuple(f"ttype{n}" for n in range(1, 9))


def get_client_fingerprint(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> dict[str, Any]:
    """
    Collect all available client information from writer.

    :param writer: TelnetWriter instance.
    :returns: Dictionary of all negotiated client attributes.
    """
    fingerprint = {}

    for key in _EXTRA_INFO_KEYS:
        value = writer.get_extra_info(key)
        if value is not None and value:
            fingerprint[key] = value

    for env_key in ("USER", "SHELL", "HOME", "PATH", "LOGNAME", "MAIL"):
        value = writer.get_extra_info(env_key)
        if value is not None and value:
            fingerprint[env_key] = value

    return fingerprint


async def _run_probe(
    writer: Union[TelnetWriter, TelnetWriterUnicode], verbose: bool = True
) -> tuple[dict[str, ProbeResult], float]:
    """Run active probe, optionally extending to MUD options."""
    if _is_maybe_ms_telnet(writer):
        probe_options = [opt for opt in CORE_OPTIONS + MUD_OPTIONS if opt[0] != NEW_ENVIRON]
        logger.info(
            "reduced probe for suspected MS telnet (ttype1=%r, ttype2=%r)",
            writer.get_extra_info("ttype1"),
            writer.get_extra_info("ttype2"),
        )
    else:
        probe_options = ALL_PROBE_OPTIONS

    total = len(probe_options)
    _writer = cast(TelnetWriterUnicode, writer)
    if verbose:
        _writer.write(f"\rProbing {total} telnet options...\x1b[J")
        await _writer.drain()

    start_time = time.time()
    results = await probe_client_capabilities(writer, options=probe_options, timeout=_PROBE_TIMEOUT)

    if _is_maybe_mud(writer) and EXTENDED_OPTIONS:
        ext_results = await probe_client_capabilities(
            writer, options=EXTENDED_OPTIONS, timeout=_PROBE_TIMEOUT
        )
        results.update(ext_results)

    elapsed = time.time() - start_time

    if verbose:
        _writer.write("\r\x1b[K")

    return results, elapsed


def _get_protocol(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> Any:
    """Return the protocol object from a writer."""
    return getattr(writer, "_protocol", None) or getattr(writer, "protocol", None)


def _opt_byte_to_name(opt: bytes) -> str:
    """Convert option bytes to name or hex string."""
    if isinstance(opt, bytes) and len(opt) > 0:
        hex_key = f"0x{opt[0]:02x}"
        return _OPT_BYTE_TO_NAME.get(hex_key, hex_key)
    return str(opt)


def _collect_option_states(
    writer: Union[TelnetWriter, TelnetWriterUnicode],
) -> dict[str, dict[str, Any]]:
    """Collect all telnet option states from writer."""
    options = {}
    for label, opt_dict in [("remote", writer.remote_option), ("local", writer.local_option)]:
        entries = {_opt_byte_to_name(opt): enabled for opt, enabled in opt_dict.items()}
        if entries:
            options[label] = entries
    return options


def _collect_rejected_options(
    writer: Union[TelnetWriter, TelnetWriterUnicode],
) -> dict[str, list[str]]:
    """Collect rejected option offers from writer."""
    result: dict[str, list[str]] = {}
    if getattr(writer, "rejected_will", None):
        result["will"] = sorted(_opt_byte_to_name(opt) for opt in writer.rejected_will)
    if getattr(writer, "rejected_do", None):
        result["do"] = sorted(_opt_byte_to_name(opt) for opt in writer.rejected_do)
    return result


def _collect_extra_info(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> dict[str, Any]:
    """Collect all extra_info from writer, including private _extra dict."""
    extra: dict[str, Any] = {}

    protocol = _get_protocol(writer)
    if protocol and hasattr(protocol, "_extra"):
        for key, value in protocol._extra.items():
            if isinstance(value, tuple):
                extra[key] = list(value)
            elif isinstance(value, bytes):
                extra[key] = value.hex()
            else:
                extra[key] = value

    # Transport-level keys not in protocol._extra
    for key in ("peername", "sockname", "timeout"):
        if key not in extra:
            if (value := writer.get_extra_info(key)) is not None:
                extra[key] = list(value) if isinstance(value, tuple) else value

    # Clean up: prefer uppercase over lowercase redundant keys
    if "TERM" in extra and "term" in extra:
        del extra["term"]
    if "COLUMNS" in extra and "cols" in extra:
        del extra["cols"]
    if "LINES" in extra and "rows" in extra:
        del extra["rows"]

    # Remove ttype1, ttype2, etc. - collected separately in ttype_cycle
    for i in range(1, 20):
        extra.pop(f"ttype{i}", None)

    return extra


def _collect_ttype_cycle(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> list[str]:
    """Collect the full TTYPE cycle responses."""
    ttype_list = []

    protocol = _get_protocol(writer)
    extra_dict = getattr(protocol, "_extra", {}) if protocol else {}

    for i in range(1, 20):
        if value := (extra_dict.get(f"ttype{i}") or writer.get_extra_info(f"ttype{i}")):
            ttype_list.append(value)
        else:
            break
    return ttype_list


def _collect_protocol_timing(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> dict[str, Any]:
    """Collect timing information from protocol."""
    timing = {}
    protocol = _get_protocol(writer)
    if protocol:
        if hasattr(protocol, "duration"):
            timing["duration"] = protocol.duration
        if hasattr(protocol, "idle"):
            timing["idle"] = protocol.idle
        if hasattr(protocol, "_connect_time"):
            timing["connect_time"] = protocol._connect_time
    return timing


def _collect_slc_tab(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> dict[str, Any]:
    """Collect non-default SLC entries when LINEMODE was negotiated."""
    slctab = getattr(writer, "slctab", None)
    if not slctab:
        return {}

    if not (hasattr(writer, "remote_option") and writer.remote_option.enabled(LINEMODE)):
        return {}

    defaults = slc.generate_slctab(slc.BSD_SLC_TAB)

    result: dict[str, Any] = {}
    slc_set: dict[str, Any] = {}
    slc_unset: list[str] = []
    slc_nosupport: list[str] = []

    for slc_func, slc_def in slctab.items():
        default_def = defaults.get(slc_func)
        if (
            default_def is not None
            and slc_def.mask == default_def.mask
            and slc_def.val == default_def.val
        ):
            continue

        name = slc.name_slc_command(slc_func)
        if slc_def.nosupport:
            slc_nosupport.append(name)
        elif slc_def.val == theNULL:
            slc_unset.append(name)
        else:
            slc_set[name] = slc_def.val[0] if isinstance(slc_def.val, bytes) else slc_def.val

    if slc_set:
        result["set"] = slc_set
    if slc_unset:
        result["unset"] = sorted(slc_unset)
    if slc_nosupport:
        result["nosupport"] = sorted(slc_nosupport)

    return result


def _create_protocol_fingerprint(
    writer: Union[TelnetWriter, TelnetWriterUnicode], probe_results: dict[str, ProbeResult]
) -> dict[str, Any]:
    """
    Create anonymized/summarized protocol fingerprint from session data.

    Fields are only included if negotiated. Environment variables are summarized as "True" (non-
    empty value) or "None" (empty string).

    :param writer: TelnetWriter instance.
    :param probe_results: Probe results from capability probing.
    :returns: Dict with anonymized protocol fingerprint data.
    """
    fingerprint: dict[str, Any] = {"probed-protocol": "client"}

    protocol = _get_protocol(writer)
    extra_dict = getattr(protocol, "_extra", {}) if protocol else {}

    for key in ("HOME", "USER", "SHELL"):
        if key in extra_dict:
            fingerprint[key] = "True" if extra_dict[key] else "None"

    # Encoding extracted from LANG
    if lang := writer.get_extra_info("LANG"):
        encoding = encoding_from_lang(lang)
        fingerprint["encoding"] = encoding if encoding else "None"
    else:
        fingerprint["encoding"] = "None"

    # TERM categorization (inlined)
    term = writer.get_extra_info("TERM") or writer.get_extra_info("term")
    if not term:
        fingerprint["TERM"] = "None"
    elif (term_lower := term.lower()) in PROTOCOL_MATCHED_TERMINALS:
        fingerprint["TERM"] = term_lower.capitalize()
    elif "ansi" in term_lower:
        fingerprint["TERM"] = "Yes-ansi"
    else:
        fingerprint["TERM"] = "Yes"

    charset = writer.get_extra_info("charset")
    fingerprint["charset"] = charset if charset else "None"

    ttype_cycle = _collect_ttype_cycle(writer)
    fingerprint["ttype-count"] = len(ttype_cycle)

    supported: list[str] = sorted(
        [name for name, info in probe_results.items() if info["status"] == "WILL"]
    )
    refused: list[str] = sorted(
        [name for name, info in probe_results.items() if info["status"] in ("WONT", "timeout")]
    )
    fingerprint["supported-options"] = supported
    fingerprint["refused-options"] = refused

    rejected = _collect_rejected_options(writer)
    if rejected.get("will"):
        fingerprint["rejected-will"] = rejected["will"]
    if rejected.get("do"):
        fingerprint["rejected-do"] = rejected["do"]

    linemode_probed = any(
        name == "LINEMODE" and info["status"] == "WILL" for name, info in probe_results.items()
    )
    if linemode_probed:
        slc_tab = _collect_slc_tab(writer)
        if slc_tab:
            fingerprint["slc"] = slc_tab

    return fingerprint


def _hash_fingerprint(data: dict[str, Any]) -> str:
    """Create deterministic 16-char SHA256 hash of a fingerprint dict."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _count_protocol_folder_files(protocol_dir: str) -> int:
    """Count JSON files in protocol fingerprint directory."""
    if not os.path.exists(protocol_dir):
        return 0
    return sum(1 for f in os.listdir(protocol_dir) if f.endswith(".json"))


def _count_fingerprint_folders(data_dir: Optional[str] = None, side: str = "client") -> int:
    """Count unique fingerprint folders in ``DATA_DIR/<side>/``."""
    _dir = data_dir if data_dir is not None else DATA_DIR
    if _dir is None:
        return 0
    side_dir = os.path.join(_dir, side)
    if not os.path.exists(side_dir):
        return 0
    return sum(1 for f in os.listdir(side_dir) if os.path.isdir(os.path.join(side_dir, f)))


def _save_fingerprint_to_dir(
    target_dir: str,
    session_hash: str,
    data: dict[str, Any],
    *,
    probe_key: str,
    data_dir: str,
    side: str,
    protocol_hash: str,
) -> Optional[str]:
    """
    Save fingerprint data to a directory with limit checks and session appending.

    Handles fingerprint-count and file-count limits, creates directories as
    needed, and appends to existing session files when the session hash matches.

    :param target_dir: Directory path for this fingerprint's files.
    :param session_hash: Hash used for the filename.
    :param data: Complete fingerprint data dict to save.
    :param probe_key: Top-level key in *data* (e.g. ``"telnet-probe"``).
    :param data_dir: Base data directory for counting fingerprint folders.
    :param side: ``"client"`` or ``"server"`` subdirectory name.
    :param protocol_hash: Protocol fingerprint hash for logging.
    :returns: Path to saved file, or ``None`` if saving was skipped.
    """
    is_new_dir = not os.path.exists(target_dir)

    if is_new_dir:
        if _count_fingerprint_folders(data_dir, side=side) >= FINGERPRINT_MAX_FINGERPRINTS:
            logger.warning(
                "max fingerprints (%d) exceeded, not saving %s",
                FINGERPRINT_MAX_FINGERPRINTS,
                protocol_hash,
            )
            return None
        try:
            os.makedirs(target_dir, exist_ok=True)
        except OSError as exc:
            logger.warning("failed to create directory %s: %s", target_dir, exc)
            return None
        logger.info("new %s fingerprint %s", side, protocol_hash)
    else:
        if _count_protocol_folder_files(target_dir) >= FINGERPRINT_MAX_FILES:
            logger.warning(
                "fingerprint %s at file limit (%d), not saving",
                protocol_hash,
                FINGERPRINT_MAX_FILES,
            )
            return None
        logger.info("connection for %s fingerprint %s", side, protocol_hash)

    filepath = os.path.join(target_dir, f"{session_hash}.json")

    if os.path.exists(filepath):
        try:
            with open(filepath, encoding="utf-8") as f:
                existing = json.load(f)
            existing[probe_key]["session_data"] = data[probe_key]["session_data"]
            existing["sessions"].append(data["sessions"][0])
        except (OSError, json.JSONDecodeError, KeyError) as exc:
            logger.warning("failed to read existing %s: %s", filepath, exc)
            existing = None

        if existing is not None:
            try:
                _atomic_json_write(filepath, existing)
                return filepath
            except OSError as exc:
                logger.warning("failed to update fingerprint: %s", exc)
                return None

    try:
        _atomic_json_write(filepath, data)
        return filepath
    except OSError as exc:
        logger.warning("failed to save fingerprint: %s", exc)
        return None


_UNKNOWN_TERMINAL_HASH = "0" * 16
AMBIGUOUS_WIDTH_UNKNOWN = -1


def _create_session_fingerprint(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> dict[str, Any]:
    """Create session identity fingerprint from stable client fields."""
    identity: dict[str, Any] = {}

    if peername := writer.get_extra_info("peername"):
        identity["client-ip"] = peername[0]

    if term := (writer.get_extra_info("TERM") or writer.get_extra_info("term")):
        identity["TERM"] = term

    for key in ("USER", "HOME", "SHELL", "LANG", "charset"):
        if (value := writer.get_extra_info(key)) is not None and value:
            identity[key] = value

    return identity


def _load_fingerprint_names(data_dir: Optional[str] = None) -> dict[str, str]:
    """Load fingerprint hash-to-name mapping from ``fingerprint_names.json``."""
    _dir = data_dir if data_dir is not None else DATA_DIR
    if _dir is None:
        return {}
    names_file = os.path.join(_dir, "fingerprint_names.json")
    if not os.path.exists(names_file):
        return {}
    with open(names_file, encoding="utf-8") as f:
        result: dict[str, str] = json.load(f)
        return result


def _save_fingerprint_name(hash_val: str, name: str, data_dir: Optional[str] = None) -> str:
    """
    Save a fingerprint hash-to-name mapping in ``fingerprint_names.json``.

    Loads the existing names file, adds or updates the entry for *hash_val*,
    and writes it back atomically.

    :param hash_val: 16-character hex fingerprint hash.
    :param name: Human-readable name to associate.
    :param data_dir: Override data directory.  Falls back to :data:`DATA_DIR`.
    :returns: Path to the saved names file.
    :raises ValueError: If *data_dir* is ``None`` and :data:`DATA_DIR` is unset.
    """
    _dir = data_dir if data_dir is not None else DATA_DIR
    if _dir is None:
        raise ValueError("no data directory configured")
    os.makedirs(_dir, exist_ok=True)
    names_file = os.path.join(_dir, "fingerprint_names.json")
    names = _load_fingerprint_names(_dir)
    names[hash_val] = name
    _atomic_json_write(names_file, names)
    return names_file


def _resolve_hash_name(hash_val: str, names: dict[str, str]) -> str:
    """Return human-readable name for a hash, falling back to the hash itself."""
    return names.get(hash_val, hash_val)


def _validate_suggestion(text: str) -> Optional[str]:
    """Validate a user-submitted fingerprint name suggestion."""
    cleaned = text.strip()
    if not cleaned:
        return None
    for c in cleaned:
        if ord(c) < 32 or ord(c) == 127:
            return None
    return cleaned


def _cooked_input(prompt: str) -> str:
    """Call :func:`input` with echo and canonical mode temporarily enabled."""
    import termios

    fd = sys.stdin.fileno()
    old_attrs = termios.tcgetattr(fd)
    new_attrs = list(old_attrs)
    new_attrs[3] |= termios.ECHO | termios.ICANON
    termios.tcsetattr(fd, termios.TCSANOW, new_attrs)
    try:
        return input(prompt)
    except EOFError:
        return ""
    finally:
        termios.tcsetattr(fd, termios.TCSANOW, old_attrs)


def _build_session_fingerprint(
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    probe_results: dict[str, ProbeResult],
    probe_time: float,
) -> dict[str, Any]:
    """Build the session fingerprint dict (raw detailed data)."""
    extra = _collect_extra_info(writer)
    extra.pop("peername", None)
    extra.pop("sockname", None)

    ttype_cycle = _collect_ttype_cycle(writer)
    option_states = _collect_option_states(writer)
    timing = _collect_protocol_timing(writer)

    linemode_probed = probe_results.get("LINEMODE", {}).get("status")
    slc_tab = _collect_slc_tab(writer) if linemode_probed == "WILL" else {}

    probe_by_status: dict[str, dict[str, int]] = {}
    for name, info in probe_results.items():
        status = info["status"]
        opt_byte = info["opt"][0] if isinstance(info["opt"], bytes) else info["opt"]
        if status not in probe_by_status:
            probe_by_status[status] = {}
        probe_by_status[status][name] = opt_byte

    timing["probe"] = probe_time

    result = {
        "extra": extra,
        "ttype_cycle": ttype_cycle,
        "option_states": option_states,
        "probe": probe_by_status,
        "timing": timing,
    }
    if slc_tab:
        result["slc_tab"] = slc_tab
    rejected = _collect_rejected_options(writer)
    if rejected:
        result["rejected"] = rejected
    if writer.comport_data:
        result["comport"] = writer.comport_data
    return result


def _save_fingerprint_data(
    writer: Union[TelnetWriter, TelnetWriterUnicode],
    probe_results: dict[str, ProbeResult],
    probe_time: float,
    session_fp: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """
    Save comprehensive fingerprint data to a JSON file.

    Creates directory structure:
    ``DATA_DIR/client/<protocol-hash>/<probe-hash>/<session_hash>.json``

    :param writer: TelnetWriter instance with full protocol access.
    :param probe_results: Probe results from capability probing.
    :param probe_time: Time taken for probing.
    :param session_fp: Pre-built session fingerprint, or None to build it.
    :returns: Path to saved file, or None if save skipped/failed.
    """
    if DATA_DIR is None:
        return None
    if not os.path.isdir(DATA_DIR):
        os.makedirs(DATA_DIR, exist_ok=True)

    if session_fp is None:
        session_fp = _build_session_fingerprint(writer, probe_results, probe_time)

    protocol_fp = _create_protocol_fingerprint(writer, probe_results)
    telnet_hash = _hash_fingerprint(protocol_fp)

    session_identity = _create_session_fingerprint(writer)
    session_hash = _hash_fingerprint(session_identity)

    telnet_dir = os.path.join(DATA_DIR, "client", telnet_hash)
    probe_dir = None
    if os.path.exists(telnet_dir):
        for name in os.listdir(telnet_dir):
            candidate = os.path.join(telnet_dir, name)
            if os.path.isdir(candidate) and name != _UNKNOWN_TERMINAL_HASH:
                probe_dir = candidate
                break
    if probe_dir is None:
        probe_dir = os.path.join(telnet_dir, _UNKNOWN_TERMINAL_HASH)

    peername = writer.get_extra_info("peername")
    now = datetime.datetime.now(datetime.timezone.utc)
    session_entry = {"ip": str(peername[0]) if peername else None, "connected": now.isoformat()}

    data = {
        "telnet-probe": {
            "fingerprint": telnet_hash,
            "fingerprint-data": protocol_fp,
            "session_data": session_fp,
        },
        "sessions": [session_entry],
    }

    return _save_fingerprint_to_dir(
        target_dir=probe_dir,
        session_hash=session_hash,
        data=data,
        probe_key="telnet-probe",
        data_dir=DATA_DIR,
        side="client",
        protocol_hash=telnet_hash,
    )


def _is_maybe_mud(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> bool:
    """Return whether the client looks like a MUD client."""
    term = (writer.get_extra_info("TERM") or "").lower()
    if term in MUD_TERMINALS:
        return True
    for key in ("ttype1", "ttype2", "ttype3"):
        if (writer.get_extra_info(key) or "").lower() in MUD_TERMINALS:
            return True
    mud_opts = (GMCP, MSDP, MXP, MSP, ATCP, AARDWOLF)
    if any(writer.remote_option.enabled(opt) for opt in mud_opts):
        return True
    return False


def _is_maybe_ms_telnet(writer: Union[TelnetWriter, TelnetWriterUnicode]) -> bool:
    """
    Return whether the client looks like Microsoft Windows telnet.

    Microsoft telnet reports ttype1="ANSI", ttype2="VT100", refuses CHARSET, and sends unsolicited
    WILL NAWS.  The ttype cycle stalls after VT100.  Sending a large NEW_ENVIRON sub-negotiation or
    a burst of legacy IAC DO commands crashes the client.

    :param writer: TelnetWriter instance.
    """
    ttype1 = (writer.get_extra_info("ttype1") or "").upper()
    if ttype1 != "ANSI":
        return False
    ttype2 = (writer.get_extra_info("ttype2") or "").upper()
    if ttype2 and ttype2 != "VT100":
        return False
    return True


async def fingerprinting_server_shell(
    reader: Union[TelnetReader, TelnetReaderUnicode],
    writer: Union[TelnetWriter, TelnetWriterUnicode],
) -> None:
    """
    Shell that probes client telnet capabilities and runs post-script.

    Immediately probes all telnet options on connect. If DATA_DIR is configured, saves fingerprint
    data and runs the post-script through a PTY so it can probe the client's terminal with ucs-
    detect.

    :param reader: TelnetReader instance.
    :param writer: TelnetWriter instance.
    """
    from .server_pty_shell import pty_shell

    writer = cast(TelnetWriterUnicode, writer)
    probe_results, probe_time = await _run_probe(writer, verbose=False)

    # Switch syncterm to Topaz (Amiga) font, just for fun why not
    if (writer.get_extra_info("TERM") or "").lower() == "syncterm":
        writer.write("\x1b[0;40 D")
        await writer.drain()

    # Collect fingerprint data BEFORE disabling LINEMODE, so that
    # _collect_slc_tab sees remote_option[LINEMODE] as True.
    session_fp = _build_session_fingerprint(writer, probe_results, probe_time)
    filepath = _save_fingerprint_data(writer, probe_results, probe_time, session_fp)

    # Disable LINEMODE if it was negotiated - stay in kludge mode (SGA+ECHO)
    # for PTY shell. LINEMODE causes echo loops with GNU telnet when running
    # ucs-detect (client's LIT_ECHO + PTY echo = feedback loop).
    if probe_results.get("LINEMODE", {}).get("status") == "WILL":
        writer.iac(DONT, LINEMODE)
        await writer.drain()
        await asyncio.sleep(0.1)

    if filepath is not None:
        post_script = FINGERPRINT_POST_SCRIPT or "telnetlib3.fingerprinting_display"
        await pty_shell(
            reader,
            writer,
            sys.executable,
            ["-W", "ignore::RuntimeWarning:runpy", "-m", post_script, str(filepath)],
            raw_mode=True,
        )
    else:
        writer.close()


def fingerprinting_post_script(filepath: str) -> None:
    """
    Post-fingerprint script that optionally runs ucs-detect for terminal probing.

    If ucs-detect is available in PATH, runs it to collect terminal capabilities
    and merges the results into the fingerprint data.

    Can be used as the TELNETLIB3_FINGERPRINT_POST_SCRIPT target::

        TELNETLIB3_FINGERPRINT_POST_SCRIPT=telnetlib3.fingerprinting
        TELNETLIB3_DATA_DIR=./data
        telnetlib3-server --shell fingerprinting_server_shell

    :param filepath: Path to the saved fingerprint JSON file.
    """
    from .fingerprinting_display import fingerprinting_post_script as _fps

    _fps(filepath)


def fingerprint_server_main() -> None:
    """
    Entry point for ``telnetlib3-fingerprint-server`` CLI.

    Reuses :func:`~telnetlib3.server.parse_server_args` and
    :func:`~telnetlib3.server.run_server` with
    :class:`FingerprintingServer` as the default protocol factory
    and :func:`fingerprinting_server_shell` as the default shell.

    Accepts ``--data-dir`` to set the fingerprint data directory.
    Falls back to the ``TELNETLIB3_DATA_DIR`` environment variable.
    """
    # local import is required to prevent circular imports
    from .server import _config, run_server, parse_server_args  # noqa: PLC0415

    global DATA_DIR
    # Extract --data-dir before parse_server_args() sees argv.
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument(
        "--data-dir",
        default=None,
        help="directory for fingerprint data" " (default: $TELNETLIB3_DATA_DIR)",
    )
    pre_args, remaining = pre.parse_known_args()
    sys.argv[1:] = remaining

    if pre_args.data_dir is not None:
        DATA_DIR = pre_args.data_dir

    args = parse_server_args()
    if args["shell"] is _config.shell:
        args["shell"] = fingerprinting_server_shell
    args["protocol_factory"] = FingerprintingServer
    asyncio.run(run_server(**args))


def main() -> None:
    """CLI entry point for fingerprinting post-processing."""
    if len(sys.argv) != 2:
        print(f"Usage: python -m {__name__} <filepath>", file=sys.stderr)
        sys.exit(1)
    fingerprinting_post_script(sys.argv[1])


if __name__ == "__main__":  # pragma: no cover
    main()
