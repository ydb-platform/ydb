"""
Fingerprint shell for telnet server identification.

This module runs **client-side**: it connects *to* a remote telnet server and
probes it for protocol capabilities, collects banner data and session
information, and saves fingerprint files.  Despite the ``server_`` prefix in
the module name, it fingerprints the remote *server*, not the local client.

It mirrors :mod:`telnetlib3.fingerprinting` (which fingerprints clients from
the server side).
"""

from __future__ import annotations

# std imports
import os
import re
import sys
import json
import time
import shutil
import asyncio
import logging
import datetime
import subprocess
from typing import Any, NamedTuple

# 3rd party
import wcwidth as _wcwidth
from wcwidth.escape_sequences import ZERO_WIDTH_PATTERN as _ZERO_WIDTH_STR_PATTERN

# local
from . import fingerprinting as _fps
from ._paths import _atomic_json_write
from .telopt import (
    VAR,
    MSSP,
    NAWS,
    LFLOW,
    TTYPE,
    VALUE,
    SNDLOC,
    TSPEED,
    USERVAR,
    LINEMODE,
    XDISPLOC,
    NEW_ENVIRON,
)
from .stream_reader import TelnetReader
from .stream_writer import TelnetWriter
from .fingerprinting import (
    EXTENDED_OPTIONS,
    ALL_PROBE_OPTIONS,
    QUICK_PROBE_OPTIONS,
    _hash_fingerprint,
    _opt_byte_to_name,
    _save_fingerprint_name,
    _save_fingerprint_to_dir,
    probe_client_capabilities,
)

__all__ = ("fingerprinting_client_shell", "probe_server_capabilities")

# Options where only the client sends WILL (in response to a server's DO).
# A server should never WILL these -- they describe client-side properties.
# The probe must not send DO for these; their state is already captured
# in ``server_requested`` (what the server sent DO for).
_CLIENT_ONLY_WILL = frozenset({TTYPE, TSPEED, NAWS, XDISPLOC, NEW_ENVIRON, LFLOW, LINEMODE, SNDLOC})

_BANNER_MAX_BYTES = 65536
_NEGOTIATION_SETTLE = 0.5
_BANNER_WAIT = 3.0
_POST_RETURN_WAIT = 3.0
_PROBE_TIMEOUT = 0.5
_MAX_PROMPT_REPLIES = 5
_JQ = shutil.which("jq")

# Match "yes/no", "y/n", "(Yes|No)", or "(Yn)"/"[Yn]" surrounded by
# non-alphanumeric chars (or at string boundaries).  The "(Yn)" form is
# a common BBS convention where the capital letter indicates the default.
_YN_RE = re.compile(
    rb"(?i)(?:^|[^a-zA-Z0-9])(yes/no|y/n|\(yes\|no\))(?:[^a-zA-Z0-9]|$)"
    rb"|[(\[][Yy][Nn][)\]]"
    rb"|[(\[][yY][nN][)\]]"
)

# Match "color?" prompts -- many MUDs ask if the user wants color.
_COLOR_RE = re.compile(rb"(?i)color\s*\?")

# Match numbered menu items offering UTF-8, e.g. "5) UTF-8", "[3] UTF-8",
# "2. UTF-8", or "1 ... UTF-8".  Many BBS/MUD systems present a charset
# selection menu at connect time.  The optional \S+/ prefix handles
# "Something/UTF-8" labels.
_MENU_UTF8_RE = re.compile(
    rb"[\[(]?(\d+)\s*(?:[\])]|\.{1,3})\s*(?:\S+\s*/\s*)?UTF-?8", re.IGNORECASE
)

# Match numbered menu items offering ANSI, e.g. "(1) Ansi", "[2] ANSI",
# "3. English/ANSI", or "1 ... English/ANSI".  Brackets, dot, and
# ellipsis delimiters are accepted.
_MENU_ANSI_RE = re.compile(rb"[\[(]?(\d+)\s*(?:[\])]|\.{1,3})\s*(?:\S+\s*/\s*)?ANSI", re.IGNORECASE)

# Match "gb/big5" encoding selection prompts common on Chinese BBS systems.
_GB_BIG5_RE = re.compile(rb"(?i)(?:^|[^a-zA-Z0-9])gb\s*/\s*big\s*5(?:[^a-zA-Z0-9]|$)")

# Strip ANSI/VT100 escape sequences from raw bytes for pattern matching.
# Reuse wcwidth's comprehensive pattern (CSI, OSC, DCS, APC, PM, charset, Fe, Fp).
_ANSI_STRIP_RE = re.compile(_ZERO_WIDTH_STR_PATTERN.pattern.encode("ascii"))

# Match "Press [.ESC.] twice" botcheck prompts (e.g. Mystic BBS).
_ESC_TWICE_RE = re.compile(rb"(?i)press\s+[\[<]?\.?esc\.?[\]>]?\s+twice")

# Match single "Press [ESC]" prompts without "twice" (e.g. Herbie's BBS).
_ESC_ONCE_RE = re.compile(rb"(?i)press\s+[\[<]?\.?esc\.?[\]>]?(?!\s+twice)")

# Match "HIT RETURN", "PRESS RETURN", "PRESS ENTER", "HIT ENTER", etc.
# Common on Worldgroup/MajorBBS and other vintage BBS systems.
_RETURN_PROMPT_RE = re.compile(rb"(?i)(?:hit|press)\s+(?:return|enter)\s*[:\.]?")

# Match "Press the BACKSPACE key" prompts -- standard telnet terminal
# detection (e.g. TelnetBible.com).  Respond with ASCII BS (0x08).
_BACKSPACE_KEY_RE = re.compile(rb"(?i)press\s+the\s+backspace\s+key")

# Match "press del/backspace" and "hit your backspace/delete" prompts from
# PETSCII BBS systems (e.g. Image BBS C/G detect).  Respond with PETSCII
# DEL byte (0x14).
_DEL_BACKSPACE_RE = re.compile(
    rb"(?i)(?:press|hit)\s+(?:your\s+)?"
    rb"(?:del(?:ete)?(?:\s*/\s*backspace)?|backspace(?:\s*/\s*del(?:ete)?)?)"
    rb"(?:\s+key)?\s*[:\.]?"
)

# Match "More: (Y)es, (N)o, (C)ontinuous?" pagination prompts.
# Answer "C" (Continuous) to disable pagination and collect the full banner.
_MORE_PROMPT_RE = re.compile(rb"(?i)more[:\s]*\(?[yY]\)?.*\(?[cC]\)?\s*(?:ontinuous|ont)")

# Match DSR (Device Status Report) request: ESC [ 6 n.
# Servers send this to detect ANSI-capable terminals; we reply with a
# Cursor Position Report (CPR) so the server sees us as ANSI-capable.
_DSR_RE = re.compile(rb"\x1b\[6n")

# Match SyncTERM/CTerm font selection: CSI Ps1 ; Ps2 SP D
# Reference: https://syncterm.bbsdev.net/cterm.html
# Ps1 = font page (0 = primary), Ps2 = font ID (0-255).
_SYNCTERM_FONT_RE = re.compile(rb"\x1b\[(\d+);(\d+) D")

#: Map SyncTERM font IDs to Python codec names.
#: Font IDs from CTerm spec / Synchronet SBBS / x84 SYNCTERM_FONTMAP.
SYNCTERM_FONT_ENCODINGS: dict[int, str] = {
    0: "cp437",
    1: "cp1251",
    2: "koi8-r",
    3: "iso-8859-2",
    4: "iso-8859-4",
    5: "cp866",
    6: "iso-8859-9",
    8: "iso-8859-8",
    9: "koi8-u",
    10: "iso-8859-15",
    11: "iso-8859-4",
    12: "koi8-r",
    13: "iso-8859-4",
    14: "iso-8859-5",
    16: "iso-8859-15",
    17: "cp850",
    18: "cp850",
    20: "cp1251",
    21: "iso-8859-7",
    22: "koi8-r",
    23: "iso-8859-4",
    24: "iso-8859-1",
    25: "cp866",
    26: "cp437",
    27: "cp866",
    29: "cp866",
    30: "iso-8859-1",
    31: "cp1131",
    32: "petscii",
    33: "petscii",
    34: "petscii",
    35: "petscii",
    36: "atascii",
    37: "cp437",
    38: "cp437",
    39: "cp437",
    40: "cp437",
    41: "cp437",
    42: "cp437",
}


log = logging.getLogger(__name__)


def detect_syncterm_font(data: bytes) -> str | None:
    """
    Extract encoding from a SyncTERM font selection sequence in *data*.

    Scans *data* for ``CSI Ps1 ; Ps2 SP D`` and returns the corresponding
    Python codec name from :data:`SYNCTERM_FONT_ENCODINGS`, or ``None``
    if no font sequence is found or the font ID is unrecognised.

    :param data: Raw bytes that may contain escape sequences.
    :returns: Encoding name or ``None``.
    """
    match = _SYNCTERM_FONT_RE.search(data)
    if match is None:
        return None
    font_id = int(match.group(2))
    return SYNCTERM_FONT_ENCODINGS.get(font_id)


#: Encodings where standard telnet CR+LF must be re-encoded to the
#: codec's native EOL byte.  The codec's ``encode()`` handles the
#: actual CR -> LF normalization; we just gate the re-encoding step.
_RETRO_EOL_ENCODINGS = frozenset({"atascii", "atari8bit", "atari_8bit"})


def _reencode_prompt(response: bytes, encoding: str) -> bytes:
    r"""
    Re-encode an ASCII prompt response for the server's encoding.

    For retro encodings (ATASCII), the standard ``\r\n`` line ending
    is re-encoded through the codec so the server receives its native
    EOL byte.  For all other encodings the response is returned as-is.

    :param response: ASCII prompt response bytes (e.g. ``b"yes\r\n"``).
    :param encoding: Remote server encoding name.
    :returns: Response bytes suitable for the server's encoding.
    """
    normalized = encoding.lower().replace("-", "_")
    if normalized not in _RETRO_EOL_ENCODINGS:
        return response
    try:
        text = response.decode("ascii")
        return text.encode(encoding)
    except (UnicodeDecodeError, UnicodeEncodeError, LookupError):
        return response


class _VirtualCursor:
    """
    Track virtual cursor column to generate position-aware CPR responses.

    The server's robot-check sends DSR, writes a test character, then sends
    DSR again.  It compares the two cursor positions to verify the character
    rendered at the expected width.  By tracking what the server writes
    between DSR requests and advancing the column using :func:`wcwidth.wcwidth`,
    the scanner produces CPR responses that satisfy the width check.

    When *encoding* is set to a single-byte encoding like ``cp437``, raw
    bytes are decoded with that encoding before measuring -- this gives
    correct column widths for servers that use SyncTERM font switching
    where the raw bytes are not valid UTF-8.
    """

    def __init__(self, encoding: str = "utf-8") -> None:
        self.col = 1
        self.encoding = encoding
        self.dsr_requests = 0
        self.dsr_replies = 0

    def cpr(self) -> bytes:
        """Return a CPR response (``ESC [ row ; col R``) at the current position."""
        self.dsr_replies += 1
        return f"\x1b[1;{self.col}R".encode("ascii")

    def advance(self, data: bytes) -> None:
        """
        Advance cursor column for *data* (non-DSR text from the server).

        ANSI escape sequences are stripped first so they do not contribute
        to cursor movement.  Backspace and carriage return are handled.
        Bytes are decoded using :attr:`encoding` so that single-byte
        encodings like CP437 produce the correct character widths.
        """
        stripped = _ANSI_STRIP_RE.sub(b"", data)
        try:
            text = stripped.decode(self.encoding, errors="replace")
        except (LookupError, Exception):
            text = stripped.decode("latin-1")
        for ch in text:
            cp = ord(ch)
            if cp == 0x08:  # backspace
                self.col = max(1, self.col - 1)
            elif cp == 0x0D:  # \r
                self.col = 1
            elif cp >= 0x20:
                w = _wcwidth.wcwidth(ch)
                if w > 0:
                    self.col += w


logger = logging.getLogger("telnetlib3.server_fingerprint")


def _is_display_worthy(v: Any) -> bool:
    """Return True if *v* should be kept in culled display output."""
    return v is not False and v != {} and v != [] and v != "" and v != b""


def _cull_display(obj: Any) -> Any:
    """Recursively remove empty, false-valued, and verbose entries for display."""
    if isinstance(obj, dict):
        return {k: _cull_display(v) for k, v in obj.items() if _is_display_worthy(v)}
    if isinstance(obj, list):
        return [_cull_display(item) for item in obj]
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.hex()
    return obj


def _print_json(data: dict[str, Any]) -> None:
    """Print *data* as JSON to stdout, colorized through ``jq`` when available."""
    raw = json.dumps(_cull_display(data), indent=2, sort_keys=True)
    if _JQ:
        result = subprocess.run(
            [_JQ, "-C", "."], input=raw, capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            raw = result.stdout.rstrip("\n")
    print(raw, file=sys.stdout)


class _PromptResult(NamedTuple):
    """Result of prompt detection with optional encoding override."""

    response: bytes | None
    encoding: str | None = None


def _detect_yn_prompt(banner: bytes) -> _PromptResult:
    r"""
    Return an appropriate first-prompt response based on banner content.

    ANSI escape sequences are stripped before pattern matching so that
    embedded color/cursor codes do not interfere with detection.

    Returns a :class:`_PromptResult` whose *response* is ``None`` when
    no recognizable prompt is found -- the caller should fall back to
    sending a bare ``\r\n``.  When a UTF-8 charset menu is selected,
    *encoding* is set to ``"utf-8"`` so the caller can update the
    session encoding.

    :param banner: Raw banner bytes collected before the first prompt.
    :returns: Prompt result with response bytes and optional encoding.
    """
    stripped = _ANSI_STRIP_RE.sub(b"", banner)
    if _ESC_TWICE_RE.search(stripped):
        return _PromptResult(b"\x1b\x1b")
    if _ESC_ONCE_RE.search(stripped):
        return _PromptResult(b"\x1b")
    if _MORE_PROMPT_RE.search(stripped):
        return _PromptResult(b"C\r\n")
    match = _YN_RE.search(stripped)
    if match:
        token = match.group(1)
        if token is not None and token.lower() in (b"yes/no", b"(yes|no)"):
            return _PromptResult(b"yes\r\n")
        return _PromptResult(b"y\r\n")
    if _COLOR_RE.search(stripped):
        return _PromptResult(b"y\r\n")
    menu_match = _MENU_UTF8_RE.search(stripped)
    if menu_match:
        return _PromptResult(menu_match.group(1) + b"\r\n", encoding="utf-8")
    ansi_match = _MENU_ANSI_RE.search(stripped)
    if ansi_match:
        return _PromptResult(ansi_match.group(1) + b"\r\n")
    if _GB_BIG5_RE.search(stripped):
        return _PromptResult(b"big5\r\n", encoding="big5")
    if _BACKSPACE_KEY_RE.search(stripped):
        return _PromptResult(b"\x08")
    if _DEL_BACKSPACE_RE.search(stripped):
        return _PromptResult(b"\x14")
    if _RETURN_PROMPT_RE.search(stripped):
        return _PromptResult(b"\r\n")
    return _PromptResult(None)


async def fingerprinting_client_shell(
    reader: TelnetReader,
    writer: TelnetWriter,
    *,
    host: str,
    port: int,
    save_path: str | None = None,
    silent: bool = False,
    set_name: str | None = None,
    environ_encoding: str = "ascii",
    scan_type: str = "quick",
    mssp_wait: float = 5.0,
    banner_quiet_time: float = 2.0,
    banner_max_wait: float = 8.0,
    banner_max_bytes: int = _BANNER_MAX_BYTES,
) -> None:
    """
    Client shell that fingerprints a remote telnet server.

    Designed to be used with :func:`functools.partial` to bind CLI
    arguments, then passed as the ``shell`` callback to
    :func:`~telnetlib3.client.open_connection` with ``encoding=False``.

    :param reader: Binary-mode :class:`~telnetlib3.stream_reader.TelnetReader`.
    :param writer: Binary-mode :class:`~telnetlib3.stream_writer.TelnetWriter`.
    :param host: Remote hostname or IP address.
    :param port: Remote port number.
    :param save_path: If set, write fingerprint JSON directly to this path.
    :param silent: Suppress fingerprint output to stdout.
    :param set_name: If set, store this name for the fingerprint hash in
        ``fingerprint_names.json`` without requiring moderation.
    :param environ_encoding: Encoding for NEW_ENVIRON data.  Default
        ``"ascii"`` per :rfc:`1572`; use ``"cp037"`` for EBCDIC hosts.
        When set to something other than ``"ascii"``, SyncTERM font
        switches will not override this encoding.
    :param scan_type: ``"quick"`` probes CORE + MUD options only (default);
        ``"full"`` includes all LEGACY options.
    :param mssp_wait: Max seconds since connect to wait for MSSP data.
    :param banner_quiet_time: Seconds of silence before considering the
        banner complete.
    :param banner_max_wait: Max seconds to wait for banner data.
    :param banner_max_bytes: Maximum bytes per banner read call.
    """
    writer.environ_encoding = environ_encoding
    writer._encoding_explicit = environ_encoding != "ascii"
    try:
        await _fingerprint_session(
            reader,
            writer,
            host=host,
            port=port,
            save_path=save_path,
            silent=silent,
            set_name=set_name,
            scan_type=scan_type,
            mssp_wait=mssp_wait,
            banner_quiet_time=banner_quiet_time,
            banner_max_wait=banner_max_wait,
            banner_max_bytes=banner_max_bytes,
        )
    except (ConnectionError, EOFError) as exc:
        logger.warning("%s:%d: %s", host, port, exc)
        writer.close()


async def _fingerprint_session(
    reader: TelnetReader,
    writer: TelnetWriter,
    *,
    host: str,
    port: int,
    save_path: str | None,
    silent: bool,
    set_name: str | None,
    scan_type: str = "quick",
    mssp_wait: float = 5.0,
    banner_quiet_time: float = 2.0,
    banner_max_wait: float = 8.0,
    banner_max_bytes: int = _BANNER_MAX_BYTES,
) -> None:
    """Run the fingerprint session (inner helper for error handling)."""
    start_time = time.time()
    cursor = _VirtualCursor(encoding=writer.environ_encoding)

    # 1. Let straggler negotiation settle -- read (and respond to DSR)
    #    instead of sleeping blind so early DSR requests get a CPR reply.
    settle_data = await _read_banner_until_quiet(
        reader,
        quiet_time=_NEGOTIATION_SETTLE,
        max_wait=_NEGOTIATION_SETTLE,
        max_bytes=banner_max_bytes,
        writer=writer,
        cursor=cursor,
    )

    # 2. Read banner (pre-return) -- wait until output stops
    banner_before_raw = await _read_banner_until_quiet(
        reader,
        quiet_time=banner_quiet_time,
        max_wait=banner_max_wait,
        max_bytes=banner_max_bytes,
        writer=writer,
        cursor=cursor,
    )
    banner_before = settle_data + banner_before_raw

    # 3. Respond to prompts -- some servers ask multiple questions in
    #    sequence (e.g. "color?" then a UTF-8 charset menu).  Loop up to
    #    _MAX_PROMPT_REPLIES times, stopping early when no prompt is detected
    #    or the connection is lost.
    after_chunks: list[bytes] = []
    latest_banner = banner_before
    for _prompt_round in range(_MAX_PROMPT_REPLIES):
        prompt_result = _detect_yn_prompt(latest_banner)
        detected = prompt_result.response
        # Skip if the ESC response was already sent inline during banner
        # collection (time-sensitive botcheck countdowns).
        if detected in (b"\x1b\x1b", b"\x1b") and getattr(writer, "_esc_inline", False):
            writer._esc_inline = False  # type: ignore[attr-defined]
            detected = None
        # Skip if the charset menu response was already sent inline.
        if prompt_result.encoding and getattr(writer, "_menu_inline", False):
            writer._menu_inline = False  # type: ignore[attr-defined]
            detected = None
        prompt_response = _reencode_prompt(
            detected if detected is not None else b"\r\n", writer.environ_encoding
        )
        writer.write(prompt_response)
        await writer.drain()
        # When the server presents a charset menu and we select an
        # encoding (e.g. UTF-8 or Big5), switch the session encoding
        # so that subsequent banner data is decoded correctly.
        if prompt_result.encoding:
            writer.environ_encoding = prompt_result.encoding
            cursor.encoding = prompt_result.encoding
            protocol = writer.protocol
            if protocol is not None:
                protocol.force_binary = True
        previous_banner = latest_banner
        latest_banner = await _read_banner_until_quiet(
            reader,
            quiet_time=banner_quiet_time,
            max_wait=banner_max_wait,
            max_bytes=banner_max_bytes,
            writer=writer,
            cursor=cursor,
        )
        after_chunks.append(latest_banner)
        if writer.is_closing() or not latest_banner:
            break
        # Stop when the server repeats the same banner -- it is not
        # advancing through prompts, just re-displaying the login screen.
        if latest_banner == previous_banner:
            break
        # Stop when no prompt was detected AND the new banner has no
        # prompt either (servers like Mystic BBS send a non-interactive
        # preamble before the real botcheck prompt).
        if detected is None and _detect_yn_prompt(latest_banner).response is None:
            break
    banner_after = b"".join(after_chunks)

    # 4. Snapshot option states before probing
    session_data: dict[str, Any] = {"option_states": _collect_server_option_states(writer)}

    # 5. Active probe (skip if connection already lost)
    if writer.is_closing():
        probe_results: dict[str, Any] = {}
        probe_time = 0.0
    else:
        probe_time = time.time()
        probe_results = await probe_server_capabilities(
            writer, scan_type=scan_type, timeout=_PROBE_TIMEOUT
        )
        probe_time = time.time() - probe_time

    # 5b. If server acknowledged MSSP but data hasn't arrived yet, wait.
    await _await_mssp_data(writer, start_time + mssp_wait)

    # 6. Complete session dicts
    session_data.update(
        {
            "scan_type": scan_type,
            "encoding": writer.environ_encoding,
            "banner_before_return": _format_banner(banner_before, encoding=writer.environ_encoding),
            "banner_after_return": _format_banner(banner_after, encoding=writer.environ_encoding),
            "timing": {"probe": probe_time, "total": time.time() - start_time},
            "dsr_requests": cursor.dsr_requests,
            "dsr_replies": cursor.dsr_replies,
        }
    )
    if writer.mssp_data is not None:
        session_data["mssp"] = writer.mssp_data
    session_data.update(_collect_mud_data(writer))

    session_entry: dict[str, Any] = {
        "host": host,
        "ip": (writer.get_extra_info("peername") or (host,))[0],
        "port": port,
        "connected": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    }

    # 7. Compute fingerprint once for save/name/display
    protocol_fp = _create_server_protocol_fingerprint(writer, probe_results, scan_type=scan_type)
    protocol_hash = _hash_fingerprint(protocol_fp)

    # 8. Save
    _save_server_fingerprint_data(
        writer=writer,
        probe_results=probe_results,
        session_data=session_data,
        session_entry=session_entry,
        save_path=save_path,
        scan_type=scan_type,
        protocol_fp=protocol_fp,
        protocol_hash=protocol_hash,
    )

    # 9. Set name in fingerprint_names.json
    if set_name is not None:
        try:
            _save_fingerprint_name(protocol_hash, set_name)
            logger.info("set name %r for %s", set_name, protocol_hash)
        except ValueError:
            logger.warning("--set-name requires --data-dir or $TELNETLIB3_DATA_DIR")

    # 10. Display
    if not silent:
        _print_json(
            {
                "server-probe": {
                    "fingerprint": protocol_hash,
                    "fingerprint-data": protocol_fp,
                    "session_data": session_data,
                },
                "sessions": [session_entry],
            }
        )

    # 10. Close
    writer.close()


async def probe_server_capabilities(
    writer: TelnetWriter,
    options: list[tuple[bytes, str, str]] | None = None,
    timeout: float = 0.5,
    scan_type: str = "quick",
) -> dict[str, _fps.ProbeResult]:
    """
    Actively probe a remote server for telnet capability support.

    Sends ``IAC DO`` for all options not yet negotiated, then waits
    for ``WILL``/``WONT`` responses.  Delegates to
    :func:`~telnetlib3.fingerprinting.probe_client_capabilities`.

    :param writer: :class:`~telnetlib3.stream_writer.TelnetWriter` instance.
    :param options: List of ``(opt_bytes, name, description)`` tuples.
        Defaults to option list based on *scan_type*, minus client-only
        options.
    :param timeout: Seconds to wait for all responses.
    :param scan_type: ``"quick"`` probes CORE + MUD options only;
        ``"full"`` includes LEGACY options.  Default ``"quick"``.
    :returns: Dict mapping option name to status dict.
    """
    if options is None:
        base = ALL_PROBE_OPTIONS if scan_type == "full" else QUICK_PROBE_OPTIONS
        # Servers handle unknown options gracefully, so always include
        # EXTENDED_OPTIONS (MUD protocols with high byte values).
        base = base + EXTENDED_OPTIONS
        options = [(opt, name, desc) for opt, name, desc in base if opt not in _CLIENT_ONLY_WILL]
    return await probe_client_capabilities(writer, options=options, timeout=timeout)


def _parse_environ_send(raw: bytes) -> list[dict[str, Any]]:
    """
    Parse a raw NEW_ENVIRON SEND payload into structured entries.

    :param raw: Bytes following ``SB NEW_ENVIRON SEND`` up to ``SE``.
    :returns: List of dicts, each with ``type`` (``"VAR"`` or ``"USERVAR"``),
        ``name`` (ASCII text portion), and optionally ``data_hex`` for
        trailing binary bytes.
    """
    entries: list[dict[str, Any]] = []
    delimiters = {VAR[0], USERVAR[0]}
    value_byte = VALUE[0]

    # Per RFC 1572: bare SEND with no VAR/USERVAR list means "send all"
    if not raw:
        return [{"type": "VAR", "name": "*"}, {"type": "USERVAR", "name": "*"}]

    # find positions of VAR/USERVAR delimiters
    breaks = [i for i, b in enumerate(raw) if b in delimiters]

    for idx, ptr in enumerate(breaks):
        kind = "VAR" if raw[ptr : ptr + 1] == VAR else "USERVAR"
        start = ptr + 1
        end = breaks[idx + 1] if idx + 1 < len(breaks) else len(raw)
        chunk = raw[start:end]

        if not chunk:
            # bare VAR or USERVAR with no name = "send all"
            entries.append({"type": kind, "name": "*"})
            continue

        # split on VALUE byte if present
        if value_byte in chunk:
            name_part, val_part = chunk.split(bytes([value_byte]), 1)
        else:
            name_part = chunk
            val_part = b""

        # contiguous ASCII-printable prefix only; trailing binary is ignored
        ascii_end = 0
        for i, b in enumerate(name_part):
            if 0x20 <= b < 0x7F:
                ascii_end = i + 1
            else:
                break
        name_text = name_part[:ascii_end].decode("ascii") if ascii_end else ""

        entry: dict[str, Any] = {"type": kind, "name": name_text}
        if val_part:
            entry["value_hex"] = val_part.hex()
        entries.append(entry)

    return entries


def _collect_server_option_states(writer: TelnetWriter) -> dict[str, dict[str, Any]]:
    """
    Collect telnet option states from the server perspective.

    :param writer: :class:`~telnetlib3.stream_writer.TelnetWriter` instance.
    :returns: Dict with ``server_offered`` (server WILL) and
        ``server_requested`` (server DO) entries.
    """
    server_offered: dict[str, Any] = {}
    for opt, enabled in writer.remote_option.items():
        server_offered[_opt_byte_to_name(opt)] = enabled

    server_requested: dict[str, Any] = {}
    for opt, enabled in writer.local_option.items():
        server_requested[_opt_byte_to_name(opt)] = enabled

    result: dict[str, Any] = {
        "server_offered": server_offered,
        "server_requested": server_requested,
    }

    if writer.environ_send_raw is not None:
        result["environ_requested"] = _parse_environ_send(writer.environ_send_raw)

    return result


def _create_server_protocol_fingerprint(
    writer: TelnetWriter, probe_results: dict[str, _fps.ProbeResult], scan_type: str = "quick"
) -> dict[str, Any]:
    """
    Create anonymized protocol fingerprint for a remote server.

    :param writer: :class:`~telnetlib3.stream_writer.TelnetWriter` instance.
    :param probe_results: Results from :func:`probe_server_capabilities`.
    :param scan_type: ``"quick"`` or ``"full"`` probe depth used.
    :returns: Deterministic fingerprint dict suitable for hashing.
    """
    offered = sorted(name for name, info in probe_results.items() if info["status"] == "WILL")
    refused = sorted(
        name for name, info in probe_results.items() if info["status"] in ("WONT", "timeout")
    )

    requested = sorted(
        _opt_byte_to_name(opt) for opt, enabled in writer.local_option.items() if enabled
    )

    return {
        "probed-protocol": "server",
        "scan-type": scan_type,
        "offered-options": offered,
        "requested-options": requested,
        "refused-options": refused,
    }


def _collect_mud_data(writer: TelnetWriter) -> dict[str, Any]:
    """Collect MUD protocol data from *writer* into a dict."""
    result: dict[str, Any] = {}
    if writer.zmp_data:
        result["zmp"] = writer.zmp_data
    if writer.atcp_data:
        result["atcp"] = [{"package": pkg, "value": val} for pkg, val in writer.atcp_data]
    if writer.aardwolf_data:
        result["aardwolf"] = writer.aardwolf_data
    if writer.mxp_data:
        result["mxp"] = [d.hex() if d else "activated" for d in writer.mxp_data]
    if writer.comport_data:
        result["comport"] = writer.comport_data
    return result


def _save_server_fingerprint_data(
    writer: TelnetWriter,
    probe_results: dict[str, _fps.ProbeResult],
    session_data: dict[str, Any],
    session_entry: dict[str, Any],
    *,
    save_path: str | None = None,
    scan_type: str = "quick",
    protocol_fp: dict[str, Any] | None = None,
    protocol_hash: str | None = None,
) -> str | None:
    """
    Save server fingerprint data to a JSON file.

    Directory structure: ``DATA_DIR/server/<protocol_hash>/<session_hash>.json``

    :param writer: :class:`~telnetlib3.stream_writer.TelnetWriter` instance.
    :param probe_results: Results from :func:`probe_server_capabilities`.
    :param session_data: Pre-built dict with ``option_states``,
        ``banner_before_return``, ``banner_after_return``, and
        ``timing`` keys.
    :param session_entry: Pre-built dict with ``host``, ``ip``,
        ``port``, and ``connected`` keys.
    :param save_path: If set, write directly to this path.
    :param scan_type: ``"quick"`` or ``"full"`` probe depth used.
    :param protocol_fp: Pre-computed protocol fingerprint dict.
    :param protocol_hash: Pre-computed fingerprint hash string.
    :returns: Path to saved file, or ``None`` if saving was skipped.
    """
    if protocol_fp is None:
        protocol_fp = _create_server_protocol_fingerprint(
            writer, probe_results, scan_type=scan_type
        )
    if protocol_hash is None:
        protocol_hash = _hash_fingerprint(protocol_fp)

    data: dict[str, Any] = {
        "server-probe": {
            "fingerprint": protocol_hash,
            "fingerprint-data": protocol_fp,
            "session_data": session_data,
        },
        "sessions": [session_entry],
    }

    # Direct save path
    if save_path is not None:
        save_dir = os.path.dirname(save_path)
        if save_dir:
            os.makedirs(save_dir, exist_ok=True)
        _atomic_json_write(save_path, data)
        logger.info("saved server fingerprint to %s", save_path)
        return save_path

    # DATA_DIR-based save
    data_dir = _fps.DATA_DIR
    if data_dir is None:
        return None
    if not os.path.isdir(data_dir):
        os.makedirs(data_dir, exist_ok=True)

    session_identity = {
        "host": session_entry["host"],
        "port": session_entry["port"],
        "ip": session_entry["ip"],
    }
    session_hash = _hash_fingerprint(session_identity)
    server_dir = os.path.join(data_dir, "server", protocol_hash)

    return _save_fingerprint_to_dir(
        target_dir=server_dir,
        session_hash=session_hash,
        data=data,
        probe_key="server-probe",
        data_dir=data_dir,
        side="server",
        protocol_hash=protocol_hash,
    )


def _format_banner(data: bytes, encoding: str = "utf-8") -> str:
    r"""
    Format raw banner bytes for JSON serialization.

    Default ``"utf-8"`` is intentional -- banners are typically UTF-8
    regardless of ``environ_encoding``; callers may override.

    Uses ``surrogateescape`` so high bytes (common in CP437 BBS art)
    are preserved as surrogates (e.g. byte ``0xB1`` -> ``U+DCB1``)
    rather than replaced with ``U+FFFD``.  JSON serialization escapes
    them as ``\udcXX``, which round-trips through :func:`json.load`.

    Falls back to ``latin-1`` when the requested encoding is unavailable
    (e.g. a server-advertised charset that Python does not recognise).

    When *encoding* is ``petscii``, inline PETSCII color control codes
    are translated to ANSI 24-bit RGB SGR sequences using the VIC-II
    C64 palette so the saved banner is human-readable with colors.

    :param data: Raw bytes from the server.
    :param encoding: Character encoding to use for decoding.
    :returns: Decoded text string (raw bytes preserved as surrogates).
    """
    try:
        text = data.decode(encoding, errors="surrogateescape")
    except LookupError:
        text = data.decode("latin-1")

    if encoding.lower() in ("petscii", "cbm", "commodore", "c64", "c128"):
        from .color_filter import PetsciiColorFilter

        text = PetsciiColorFilter().filter(text)
        # PETSCII uses CR (0x0D) as line terminator; normalize to LF.
        text = text.replace("\r\n", "\n").replace("\r", "\n")

    return text


async def _await_mssp_data(writer: TelnetWriter, deadline: float) -> None:
    """Wait for MSSP data until *deadline* if server acknowledged MSSP."""
    if not writer.remote_option.enabled(MSSP) or writer.mssp_data is not None:
        return
    remaining = deadline - time.time()
    while remaining > 0 and writer.mssp_data is None:
        await asyncio.sleep(min(0.05, remaining))
        remaining = deadline - time.time()


async def _read_banner(
    reader: TelnetReader, timeout: float = _BANNER_WAIT, max_bytes: int = _BANNER_MAX_BYTES
) -> bytes:
    """
    Read up to *max_bytes* from *reader* with timeout.

    Returns whatever bytes were received before the timeout, which may
    be empty if the server sends nothing.

    :param reader: :class:`~telnetlib3.stream_reader.TelnetReader` instance.
    :param timeout: Seconds to wait for data.
    :param max_bytes: Maximum bytes to read in a single call.
    :returns: Banner bytes (may be empty).
    """
    try:
        data = await asyncio.wait_for(reader.read(max_bytes), timeout=timeout)
    except (asyncio.TimeoutError, EOFError):
        data = b""
    return data


def _respond_to_dsr(chunk: bytes, writer: TelnetWriter, cursor: _VirtualCursor | None) -> None:
    """
    Send CPR response(s) for each DSR found in *chunk*.

    When *cursor* is provided, text between DSR sequences advances the
    virtual cursor column so each CPR reflects the correct position.
    Without a cursor, a static ``ESC [ 1 ; 1 R`` is sent for every DSR.
    """
    if cursor is None:
        for _ in _DSR_RE.finditer(chunk):
            writer.write(b"\x1b[1;1R")
        return
    pos = 0
    for match in _DSR_RE.finditer(chunk):
        cursor.dsr_requests += 1
        cursor.advance(chunk[pos : match.start()])
        writer.write(cursor.cpr())
        pos = match.end()
    cursor.advance(chunk[pos:])


async def _read_banner_until_quiet(
    reader: TelnetReader,
    quiet_time: float = 2.0,
    max_wait: float = 8.0,
    max_bytes: int = _BANNER_MAX_BYTES,
    writer: TelnetWriter | None = None,
    cursor: _VirtualCursor | None = None,
) -> bytes:
    """
    Read banner data until output stops for *quiet_time* seconds.

    Keeps reading chunks as they arrive.  If no new data appears within
    *quiet_time* seconds (or *max_wait* total elapses), returns everything
    collected so far.

    When *writer* is provided, any DSR (Device Status Report) request
    ``ESC [ 6 n`` found in the incoming data is answered with a CPR
    (Cursor Position Report) so the server detects an ANSI-capable
    terminal.

    When *cursor* is provided, the CPR response reflects the tracked
    virtual cursor column (advanced by :func:`wcwidth.wcwidth` for each
    printable character).  This defeats robot-check guards that verify
    cursor movement after writing a test character.

    Time-sensitive prompts -- ``Press [.ESC.] twice`` botcheck countdowns
    and charset selection menus -- are detected inline and responded to
    immediately so the reply arrives before the server times out.

    :param reader: :class:`~telnetlib3.stream_reader.TelnetReader` instance.
    :param quiet_time: Seconds of silence before considering banner complete.
    :param max_wait: Maximum total seconds to wait for banner data.
    :param max_bytes: Maximum bytes per read call.
    :param writer: Optional :class:`~telnetlib3.stream_writer.TelnetWriter`
        used to send CPR replies to DSR requests.
    :param cursor: Optional :class:`_VirtualCursor` for position-aware CPR.
    :returns: Banner bytes (may be empty).
    """
    chunks: list[bytes] = []
    stripped_accum = bytearray()
    esc_responded = False
    menu_responded = False
    loop = asyncio.get_event_loop()
    deadline = loop.time() + max_wait
    while loop.time() < deadline:
        remaining = min(quiet_time, deadline - loop.time())
        if remaining <= 0:
            break
        try:
            chunk = await asyncio.wait_for(reader.read(max_bytes), timeout=remaining)
            if not chunk:
                break
            if writer is not None and _DSR_RE.search(chunk):
                _respond_to_dsr(chunk, writer, cursor)
                await writer.drain()
            elif cursor is not None:
                cursor.advance(chunk)
            if writer is not None:
                font_enc = detect_syncterm_font(chunk)
                if font_enc is not None:
                    log.debug("SyncTERM font switch detected: %s", font_enc)
                    if getattr(writer, "_encoding_explicit", False):
                        log.debug(
                            "ignoring font switch, explicit encoding: %s", writer.environ_encoding
                        )
                    else:
                        writer.environ_encoding = font_enc
                        if cursor is not None:
                            cursor.encoding = font_enc
                    protocol = writer.protocol
                    if protocol is not None:
                        protocol.force_binary = True
                stripped_chunk = _ANSI_STRIP_RE.sub(b"", chunk)
                stripped_accum.extend(stripped_chunk)
                if not esc_responded:
                    if _ESC_TWICE_RE.search(stripped_chunk):
                        writer.write(b"\x1b\x1b")
                        await writer.drain()
                        esc_responded = True
                        writer._esc_inline = True  # type: ignore[attr-defined]
                    elif _ESC_ONCE_RE.search(stripped_chunk):
                        writer.write(b"\x1b")
                        await writer.drain()
                        esc_responded = True
                        writer._esc_inline = True  # type: ignore[attr-defined]
                if not menu_responded:
                    menu_match = _MENU_UTF8_RE.search(stripped_accum)
                    if menu_match:
                        response = menu_match.group(1) + b"\r\n"
                        writer.write(_reencode_prompt(response, writer.environ_encoding))
                        await writer.drain()
                        menu_responded = True
                        log.debug("inline UTF-8 menu response: %r", response)
                        if not getattr(writer, "_encoding_explicit", False):
                            writer.environ_encoding = "utf-8"
                            if cursor is not None:
                                cursor.encoding = "utf-8"
                        protocol = writer.protocol
                        if protocol is not None:
                            protocol.force_binary = True
                        writer._menu_inline = True  # type: ignore[attr-defined]
            chunks.append(chunk)
        except (asyncio.TimeoutError, EOFError):
            break
    return b"".join(chunks)
