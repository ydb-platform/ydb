"""
Display, REPL, and post-script functions for telnet fingerprinting.

This module contains all terminal display (blessed/prettytable), ucs-detect
integration, and interactive REPL code split from :mod:`fingerprinting`.
"""

# std imports
import os
import sys
import copy
import json
import random
import shutil
import logging
import termios
import tempfile
import textwrap
import warnings
import functools
import contextlib
import subprocess
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Optional, Generator

if TYPE_CHECKING:
    import blessed
    import prettytable

# local
from ._paths import _atomic_json_write
from .accessories import PATIENCE_MESSAGES
from .fingerprinting import (
    DATA_DIR,
    _UNKNOWN_TERMINAL_HASH,
    AMBIGUOUS_WIDTH_UNKNOWN,
    _cooked_input,
    _hash_fingerprint,
    _resolve_hash_name,
    _validate_suggestion,
    _load_fingerprint_names,
)

__all__ = ("fingerprinting_post_script",)

logger = logging.getLogger("telnetlib3.fingerprint")

_BAT = shutil.which("bat") or shutil.which("batcat")
_JQ = shutil.which("jq")

echo = functools.partial(print, end="", flush=True)


def _run_ucs_detect() -> Optional[Dict[str, Any]]:
    """Run ucs-detect if available and return terminal fingerprint data."""
    ucs_detect = shutil.which("ucs-detect")
    if not ucs_detect:
        return None

    patience_msg = random.choice(PATIENCE_MESSAGES)
    echo(f"{patience_msg}...\r\n")

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp:
        tmp_path = tmp.name

    try:
        try:
            result = subprocess.run(
                [
                    ucs_detect,
                    "--limit-category-time=1",
                    "--limit-codepoints=10",
                    "--timeout-cps=2",
                    "--limit-errors=2",
                    "--probe-silently",
                    "--no-final-summary",
                    "--no-languages-test",
                    "--save-json",
                    tmp_path,
                ],
                timeout=20,
                check=False,
            )
        except subprocess.TimeoutExpired:
            logger.warning("ucs-detect timed out (client unresponsive to probes)")
            return None

        if result.returncode != 0:
            return None

        if not os.path.exists(tmp_path):
            logger.warning("ucs-detect did not create output file")
            return None

        with open(tmp_path, encoding="utf-8") as f:
            terminal_data = json.load(f)

        for key in ("python_version", "datetime", "system", "wcwidth_version"):
            terminal_data.pop(key, None)

        parsed: Dict[str, Any] = terminal_data
        return parsed

    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def _create_terminal_fingerprint(terminal_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create anonymized terminal fingerprint for hashing.

    Distills static terminal-identity fields from ucs-detect output, excluding session-variable data
    (colors, dimensions, timing).
    """
    fingerprint: Dict[str, Any] = {}

    results = terminal_data.get("terminal_results", {})
    fingerprint["software_name"] = terminal_data.get("software_name", "unknown")
    fingerprint["software_version"] = terminal_data.get("software_version", "unknown")

    fingerprint["number_of_colors"] = results.get("number_of_colors")
    fingerprint["sixel"] = results.get("sixel", False)
    fingerprint["iterm2_features"] = results.get("iterm2_features", {})

    fingerprint["kitty_graphics"] = results.get("kitty_graphics", False)
    fingerprint["kitty_clipboard_protocol"] = results.get("kitty_clipboard_protocol", False)
    fingerprint["kitty_keyboard"] = results.get("kitty_keyboard", {})
    fingerprint["kitty_notifications"] = results.get("kitty_notifications", False)
    fingerprint["kitty_pointer_shapes"] = results.get("kitty_pointer_shapes", False)

    fingerprint["text_sizing"] = results.get("text_sizing", {})

    da = results.get("device_attributes", {})
    fingerprint["da_service_class"] = da.get("service_class")
    fingerprint["da_extensions"] = sorted(da.get("extensions", []))

    raw_modes = results.get("modes", {})
    distilled_modes = {}
    for mode_num, mode_data in sorted(raw_modes.items(), key=lambda x: int(x[0])):
        if isinstance(mode_data, dict):
            distilled_modes[str(mode_num)] = {
                "supported": mode_data.get("supported", False),
                "changeable": mode_data.get("changeable", False),
                "enabled": mode_data.get("enabled", False),
                "value": mode_data.get("value", 0),
            }
    fingerprint["modes"] = distilled_modes

    fingerprint["xtgettcap"] = results.get("xtgettcap", {})
    fingerprint["ambiguous_width"] = terminal_data.get("ambiguous_width")

    raw_test_results = terminal_data.get("test_results", {})
    distilled_tests = {}
    for category, versions in raw_test_results.items():
        if not versions or not isinstance(versions, dict):
            continue
        for ver, entry in versions.items():
            if isinstance(entry, dict):
                distilled_tests[category] = {
                    "unicode_version": ver,
                    "n_errors": entry.get("n_errors", 0),
                    "n_total": entry.get("n_total", 0),
                }
                break
    if distilled_tests:
        fingerprint["test_results"] = distilled_tests

    return fingerprint


def _wrap_options(options: List[str], max_width: int = 30) -> str:
    """Word-wrap a list of options to fit within max_width."""
    if not options:
        return ""
    return "\n".join(textwrap.wrap(", ".join(options), width=max_width))


def _color_yes_no(term: "blessed.Terminal", value: bool) -> str:
    """Apply green/red coloring to boolean value."""
    if value:
        return str(term.forestgreen("Yes"))
    return str(term.firebrick1("No"))


def _format_ttype(
    extra: Dict[str, Any], session_data: Dict[str, Any], wrap_width: int = 30
) -> Optional[str]:
    """Format terminal type from TTYPE cycle for compact display."""
    ttype_cycle = session_data.get("ttype_cycle", [])
    term_type = extra.get("TERM") or extra.get("term")
    if not term_type and not ttype_cycle:
        return None
    primary = ttype_cycle[0] if ttype_cycle else term_type
    primary_lower = primary.lower() if primary else ""
    others = []
    seen = {primary_lower}
    for ttype_val in ttype_cycle[1:]:
        t_lower = ttype_val.lower()
        if t_lower not in seen:
            seen.add(t_lower)
            others.append(t_lower)
    type_str = primary or ""
    if others:
        suffix = ", ".join(others)
        if len(type_str) + len(suffix) + 3 > wrap_width:
            wrapped = "\n".join(textwrap.wrap(suffix, width=wrap_width - 2))
            type_str += f" ({wrapped})"
        else:
            type_str += f" ({suffix})"
    return type_str


def _is_utf8_charset(value: str) -> bool:
    """Test whether a charset or encoding string refers to UTF-8."""
    return value.lower().replace("-", "").replace("_", "") in ("utf8", "unicode11utf8")


def _format_encoding(
    extra: Dict[str, Any], proto_data: Dict[str, Any], ambiguous_width: Optional[int] = None
) -> Optional[Tuple[str, str]]:
    """Consolidate LANG, charset, and encoding into a single key-value pair."""
    lang_val = extra.get("LANG")
    charset_val = extra.get("charset")
    encoding_val = proto_data.get("encoding")

    no_unicode = ambiguous_width == AMBIGUOUS_WIDTH_UNKNOWN

    if charset_val and no_unicode and _is_utf8_charset(charset_val):
        charset_val = "unknown (ascii-only)"

    if lang_val and charset_val:
        return ("LANG (Charset)", f"{lang_val} ({charset_val})")
    if lang_val:
        return ("LANG", lang_val)
    if charset_val:
        return ("Charset", charset_val)
    if encoding_val and encoding_val != "None":
        return ("Encoding", encoding_val)
    return None


def _build_terminal_rows(term: "blessed.Terminal", data: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Build (key, value) tuples for terminal capabilities table."""
    pairs: List[Tuple[str, str]] = []
    terminal_probe = data.get("terminal-probe", {})
    terminal_data = terminal_probe.get("session_data", {})
    terminal_results = terminal_data.get("terminal_results", {})
    if not terminal_data:
        return pairs

    if fp_hash := terminal_probe.get("fingerprint"):
        pairs.append(("Fingerprint", fp_hash))

    if software := terminal_data.get("software_name"):
        if ver := terminal_data.get("software_version"):
            software += f" {ver}"
        if len(software) > 15:
            software = software[:14] + ("\u2026" if _has_unicode(data) else "..")
        pairs.append(("Software", software))

    telnet_probe = data.get("telnet-probe", {})
    session_data = telnet_probe.get("session_data", {})
    extra = session_data.get("extra", {})
    cols = extra.get("cols") or extra.get("COLUMNS")
    rows = extra.get("rows") or extra.get("LINES")
    if cols and rows:
        size_str = f"{cols}x{rows}"
        cell_w = terminal_results.get("cell_width")
        cell_h = terminal_results.get("cell_height")
        if cell_w and cell_h:
            size_str += f" (*{cell_w}x{cell_h})"
        pairs.append(("Size", size_str))

    if (n_colors := terminal_results.get("number_of_colors")) is not None:
        if n_colors >= 16777216:
            color_str = term.forestgreen("24-bit")
        elif n_colors <= 256:
            color_str = term.firebrick1(f"{n_colors}")
        else:
            color_str = term.darkorange(f"{n_colors}")
        pairs.append(("Colors", color_str))

    has_fg = terminal_results.get("foreground_color_hex") is not None
    has_bg = terminal_results.get("background_color_hex") is not None
    if has_fg or has_bg:
        pairs.append(("fg/bg colors", _color_yes_no(term, has_fg and has_bg)))

    has_kitty_gfx = terminal_results.get("kitty_graphics", False)
    has_iterm2_gfx = (terminal_results.get("iterm2_features") or {}).get("supported", False)
    has_sixel = terminal_results.get("sixel", False)
    if has_kitty_gfx or has_iterm2_gfx:
        protocols = []
        if has_kitty_gfx:
            protocols.append("Kitty")
        if has_iterm2_gfx:
            protocols.append("iTerm2")
        if has_sixel:
            protocols.append("Sixel")
        pairs.append(("Graphics", term.forestgreen(", ".join(protocols))))
    elif has_sixel:
        pairs.append(("Graphics", term.darkorange("Sixel")))
    elif any(k in terminal_results for k in ("sixel", "kitty_graphics", "iterm2_features")):
        pairs.append(("Graphics", term.firebrick1("No")))

    if da := terminal_results.get("device_attributes"):
        if (sc := da.get("service_class")) is not None:
            class_names = {
                1: "VT100",
                2: "VT200",
                18: "VT330",
                41: "VT420",
                61: "VT500",
                62: "VT500",
                64: "VT500",
                65: "VT500",
            }
            pairs.append(("Device Class", class_names.get(sc, f"Class {sc}")))

    screen_ratio = terminal_results.get("screen_ratio")
    if screen_ratio:
        ratio_name = terminal_results.get("screen_ratio_name", "")
        if ratio_name:
            pairs.append(("Aspect Ratio", f"{screen_ratio} ({ratio_name})"))
        else:
            pairs.append(("Aspect Ratio", screen_ratio))

    ambiguous_width = terminal_data.get("ambiguous_width")
    if ambiguous_width == 2:
        pairs.append(("Ambiguous Width", "wide (2)"))

    modes = terminal_results.get("modes", {})
    mode_2027 = modes.get(2027, modes.get("2027"))
    if mode_2027 is not None:
        gc_value = _color_yes_no(term, mode_2027.get("supported"))
        pairs.append(("Graphemes(2027)", gc_value))
    elif modes:
        pairs.append(("Graphemes(2027)", term.darkorange("N/A")))

    test_results = terminal_data.get("test_results", {})
    _emoji_keys = (
        "unicode_wide_results",
        "emoji_zwj_results",
        "emoji_vs16_results",
        "emoji_vs15_results",
    )
    all_pcts = []
    for key in _emoji_keys:
        for entry in test_results.get(key, {}).values():
            if (pct := entry.get("pct_success")) is not None:
                all_pcts.append(pct)
    if all_pcts:
        avg = sum(all_pcts) / len(all_pcts)
        if avg >= 99.0:
            pairs.append(("Emoji", term.forestgreen("Yes")))
        elif avg >= 33.3:
            pairs.append(("Emoji", term.darkorange("Partial")))
        else:
            pairs.append(("Emoji", term.firebrick1("No")))

    return pairs


def _build_telnet_rows(term: "blessed.Terminal", data: Dict[str, Any]) -> List[Tuple[str, str]]:
    """Build (key, value) tuples for telnet protocol table."""
    pairs: List[Tuple[str, str]] = []
    telnet_probe = data.get("telnet-probe", {})
    proto_data = telnet_probe.get("fingerprint-data", {})
    session_data = telnet_probe.get("session_data", {})
    extra = session_data.get("extra", {})

    if fp_hash := telnet_probe.get("fingerprint"):
        pairs.append(("Fingerprint", fp_hash))

    wrap_width = 30
    if type_str := _format_ttype(extra, session_data, wrap_width):
        pairs.append(("Terminal Type", type_str))

    terminal_probe = data.get("terminal-probe", {})
    aw = terminal_probe.get("session_data", {}).get("ambiguous_width")
    if encoding_pair := _format_encoding(extra, proto_data, aw):
        pairs.append(encoding_pair)

    if supported := proto_data.get("supported-options"):
        pairs.append(("Options", _wrap_options(supported, wrap_width)))

    if rejected_will := proto_data.get("rejected-will"):
        pairs.append(("Rejected", _wrap_options(rejected_will, wrap_width)))

    slc_tab = session_data.get("slc_tab", {})
    if slc_tab:
        slc_set = slc_tab.get("set", {})
        slc_unset = slc_tab.get("unset", [])
        slc_nosupport = slc_tab.get("nosupport", [])
        parts = []
        if slc_set:
            parts.append(f"{len(slc_set)} set")
        if slc_unset:
            parts.append(f"{len(slc_unset)} unset")
        if slc_nosupport:
            parts.append(f"{len(slc_nosupport)} nosupport")
        if parts:
            pairs.append(("SLC", ", ".join(parts)))

    env_vars = []
    for key in ("USER", "HOME", "SHELL"):
        if proto_data.get(key) == "True":
            env_vars.append(key)
    if env_vars:
        pairs.append(("Environment", ", ".join(env_vars)))

    if tspeed := extra.get("tspeed"):
        pairs.append(("Speed", tspeed))

    return pairs


def _make_terminal(**kwargs: Any) -> "blessed.Terminal":
    """Create a blessed Terminal, falling back to ``ansi`` on setupterm failure."""
    from blessed import Terminal

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        term = Terminal(**kwargs)
    if any("setupterm" in str(w.message) for w in caught):
        kwargs["kind"] = "ansi"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            term = Terminal(**kwargs)
    return term


@contextlib.contextmanager
def _disable_isig() -> Generator[None, None, None]:
    """Disable ``ISIG`` so that ``^C`` and ``^Z`` are ignored."""
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    new = list(old)
    new[3] &= ~termios.ISIG
    termios.tcsetattr(fd, termios.TCSANOW, new)
    try:
        yield
    finally:
        termios.tcsetattr(fd, termios.TCSANOW, old)


def _has_unicode(data: Dict[str, Any]) -> bool:
    """Return whether the terminal supports unicode rendering."""
    aw = (
        data.get("terminal-probe", {})
        .get("session_data", {})
        .get("ambiguous_width", AMBIGUOUS_WIDTH_UNKNOWN)
    )
    return bool(aw >= 1)


def _sync_timeout(data: Dict[str, Any]) -> float:
    """Return synchronized output timeout based on measured RTT."""
    cps = data.get("terminal-probe", {}).get("session_data", {}).get("cps_summary", {})
    if (rtt_max := cps.get("rtt_max_ms")) and rtt_max > 0:
        return float(rtt_max * 1.1 / 1000.0)
    return 1.0


def _setup_term_environ(data: Dict[str, Any]) -> None:
    """
    Set ``TERM`` and ``COLORTERM`` based on probe data.

    Overrides ``TERM`` to ``ansi`` for Microsoft telnet clients whose
    ``vtnt`` terminfo contains ``$<N>`` padding sequences displayed as
    literal text.  Sets ``COLORTERM=truecolor`` when 24-bit color was
    confirmed by the terminal probe, removes it otherwise to prevent the
    server's own stale value from leaking through.
    """
    ttype_cycle = data.get("telnet-probe", {}).get("session_data", {}).get("ttype_cycle", [])
    # Microsoft telnet cycles ANSI -> VT100 -> VT52 -> VTNT -> VTNT.
    # The "vtnt" terminfo contains $<N> padding sequences that MS telnet
    # displays as literal text.  Override to "ansi" which has no padding.
    if ttype_cycle == ["ANSI", "VT100", "VT52", "VTNT", "VTNT"]:
        os.environ["TERM"] = "ansi"

    if _has_truecolor(data):
        os.environ["COLORTERM"] = "truecolor"
    else:
        os.environ.pop("COLORTERM", None)


def _has_truecolor(data: Dict[str, Any]) -> bool:
    """Return whether the terminal supports 24-bit color."""
    n = (
        data.get("terminal-probe", {})
        .get("session_data", {})
        .get("terminal_results", {})
        .get("number_of_colors")
    )
    return n is not None and n >= 16777216


def _hotkey(term: "blessed.Terminal", key: str) -> str:
    """Format a hotkey as ``key-`` with key and dash in magenta."""
    return f"{term.bold_magenta(key)}{term.bold_magenta('-')}"


def _bracket_key(term: "blessed.Terminal", key: str) -> str:
    """Format a hotkey as ``[key]`` with brackets in cyan, key in magenta."""
    return f"{term.cyan('[')}{term.bold_magenta(key)}{term.cyan(']')}"


def _apply_unicode_borders(tbl: "prettytable.PrettyTable") -> None:
    """Apply double-line box-drawing characters to a PrettyTable."""
    tbl.horizontal_char = "\u2550"
    tbl.vertical_char = "\u2551"
    tbl.junction_char = "\u256c"
    tbl.top_junction_char = "\u2566"
    tbl.bottom_junction_char = "\u2569"
    tbl.left_junction_char = "\u2560"
    tbl.right_junction_char = "\u2563"
    tbl.top_left_junction_char = "\u2554"
    tbl.top_right_junction_char = "\u2557"
    tbl.bottom_left_junction_char = "\u255a"
    tbl.bottom_right_junction_char = "\u255d"


def _display_compact_summary(
    data: Dict[str, Any], term: Optional["blessed.Terminal"] = None
) -> bool:
    """Display compact fingerprint summary using prettytable."""
    try:
        from ucs_detect import _collect_side_by_side_lines
        from prettytable import PrettyTable
    except ImportError:
        return False

    if term is None:
        term = _make_terminal()

    has_unicode = _has_unicode(data)

    def make_table(title: str, pairs: List[Tuple[str, str]]) -> str:
        tbl = PrettyTable()
        if has_unicode:
            _apply_unicode_borders(tbl)
        tbl.title = term.magenta(title)
        tbl.field_names = ["Attribute", "Value"]
        tbl.align["Attribute"] = "r"
        tbl.align["Value"] = "l"
        tbl.header = False
        tbl.max_table_width = max(40, (term.width or 80) - 1)
        for key, value in pairs:
            tbl.add_row([key or "", value])
        return str(tbl)

    table_strings = []

    terminal_rows = _build_terminal_rows(term, data)
    if terminal_rows:
        table_strings.append(make_table("Terminal", terminal_rows))

    telnet_rows = _build_telnet_rows(term, data)
    if telnet_rows:
        table_strings.append(make_table("Telnet", telnet_rows))

    if not table_strings:
        return False

    timeout = _sync_timeout(data)

    echo(term.normal)

    widths = [len(s.split("\n", 1)[0]) for s in table_strings]
    side_by_side = len(widths) < 2 or sum(widths) + 1 < (term.width or 80)

    if side_by_side:
        all_lines = _collect_side_by_side_lines(term, table_strings)
        if has_unicode:
            with term.synchronized_output(timeout=timeout):
                for line in all_lines:
                    echo(line + "\n")
        else:
            for line in all_lines:
                echo(line + "\n")
    else:
        for tbl in table_strings:
            lines = tbl.split("\n")
            if has_unicode:
                with term.synchronized_output(timeout=timeout):
                    for line in lines:
                        echo(line + "\n")
                    echo("\n")
            else:
                for line in lines:
                    echo(line + "\n")
                echo("\n")
    return True


def _fingerprint_similarity(a: Dict[str, Any], b: Dict[str, Any]) -> float:
    """
    Compute field-by-field similarity score between two fingerprint dicts.

    :returns: Similarity as a float 0.0-1.0.
    """
    _skip = {"probed-protocol"}
    all_keys = (set(a) | set(b)) - _skip
    if not all_keys:
        return 1.0

    scores: List[float] = []
    for key in all_keys:
        va, vb = a.get(key), b.get(key)
        if va is None and vb is None:
            continue
        if va is None or vb is None:
            scores.append(0.0)
            continue
        if va == vb:
            scores.append(1.0)
            continue
        if isinstance(va, list) and isinstance(vb, list):
            sa, sb = set(map(str, va)), set(map(str, vb))
            union = sa | sb
            scores.append(len(sa & sb) / len(union) if union else 1.0)
        elif isinstance(va, dict) and isinstance(vb, dict):
            scores.append(_fingerprint_similarity(va, vb))
        else:
            scores.append(0.0)

    return sum(scores) / len(scores) if scores else 1.0


def _load_known_fingerprints(probe_type: str) -> Dict[str, Dict[str, Any]]:
    """
    Load one fingerprint-data dict per unique hash from the data directory.

    :param probe_type: ``"telnet-probe"`` or ``"terminal-probe"``.
    :returns: Dict mapping hash string to fingerprint-data dict.
    """
    if DATA_DIR is None:
        return {}
    client_dir = os.path.join(DATA_DIR, "client")
    if not os.path.isdir(client_dir):
        return {}

    seen: Dict[str, Dict[str, Any]] = {}
    is_telnet = probe_type == "telnet-probe"

    for telnet_hash in os.listdir(client_dir):
        telnet_path = os.path.join(client_dir, telnet_hash)
        if not os.path.isdir(telnet_path):
            continue
        if is_telnet and telnet_hash in seen:
            continue
        for terminal_hash in os.listdir(telnet_path):
            if terminal_hash == _UNKNOWN_TERMINAL_HASH:
                continue
            terminal_path = os.path.join(telnet_path, terminal_hash)
            if not os.path.isdir(terminal_path):
                continue
            if not is_telnet and terminal_hash in seen:
                continue
            target_hash = telnet_hash if is_telnet else terminal_hash
            if target_hash in seen:
                continue
            for fname in os.listdir(terminal_path):
                if not fname.endswith(".json"):
                    continue
                try:
                    with open(os.path.join(terminal_path, fname), encoding="utf-8") as f:
                        file_data = json.load(f)
                    fp_data = file_data.get(probe_type, {}).get("fingerprint-data")
                    if fp_data:
                        seen[target_hash] = fp_data
                except (OSError, json.JSONDecodeError, KeyError):
                    pass
                break
    return seen


def _find_nearest_match(
    fp_data: Dict[str, Any], probe_type: str, names: Dict[str, str]
) -> Optional[Tuple[str, float]]:
    """
    Find the most similar named fingerprint.

    :returns: ``(name, similarity)`` tuple or None if no candidates or best < 50%.
    """
    known = _load_known_fingerprints(probe_type)
    best_name: Optional[str] = None
    best_score = 0.0
    for h, known_fp in known.items():
        if h not in names:
            continue
        score = _fingerprint_similarity(fp_data, known_fp)
        if score > best_score:
            best_score = score
            best_name = names[h]
    if best_name is None or best_score < 0.50:
        return None
    return (best_name, best_score)


def _build_seen_counts(
    data: Dict[str, Any],
    names: Optional[Dict[str, str]] = None,
    term: Optional["blessed.Terminal"] = None,
) -> str:
    """Build friendly "seen before" text from folder and session counts."""
    if DATA_DIR is None or not os.path.exists(DATA_DIR):
        return ""

    telnet_probe = data.get("telnet-probe", {})
    if not (telnet_hash := telnet_probe.get("fingerprint")):
        return ""

    terminal_probe = data.get("terminal-probe", {})
    terminal_hash = terminal_probe.get("fingerprint", _UNKNOWN_TERMINAL_HASH)

    _names = names or {}
    telnet_name = _resolve_hash_name(telnet_hash, _names)
    terminal_known = terminal_hash != _UNKNOWN_TERMINAL_HASH
    terminal_name = _resolve_hash_name(terminal_hash, _names) if terminal_known else None

    if term is not None:
        g = term.forestgreen
        telnet_name = g(telnet_name)
        if terminal_name is not None:
            terminal_name = g(terminal_name)

    telnet_dir = os.path.join(DATA_DIR, "client", telnet_hash)
    like_count = 0
    if os.path.isdir(telnet_dir):
        for sub in os.listdir(telnet_dir):
            sub_path = os.path.join(telnet_dir, sub)
            if os.path.isdir(sub_path):
                like_count += sum(1 for f in os.listdir(sub_path) if f.endswith(".json"))

    visit_count = len(data.get("sessions", []))

    extra = telnet_probe.get("session_data", {}).get("extra", {})
    username = extra.get("USER") or extra.get("LOGNAME")

    lines: List[str] = []
    if like_count > 1:
        others = like_count - 1
        noun = "client" if others == 1 else "clients"
        lines.append(f"I've seen {others} other {noun} with your configuration.")
    if visit_count > 1:
        times = "time" if visit_count - 1 == 1 else "times"
        lines.append(f"I've seen your exact fingerprint {visit_count - 1} {times} before.")

    who = f" {username}" if username else ""
    terminal_suffix = (
        f" and {terminal_name}" if terminal_name and terminal_name != telnet_name else ""
    )
    if visit_count <= 1:
        lines.append(f"Welcome{who}! Detected {telnet_name}{terminal_suffix}.")
    else:
        lines.append(f"Welcome back{who}! Detected {telnet_name}{terminal_suffix}.")

    telnet_unknown = telnet_hash not in _names
    terminal_unknown = terminal_known and terminal_hash not in _names
    if (telnet_unknown or terminal_unknown) and _names:
        match_lines = _nearest_match_lines(
            data, _names, term, telnet_unknown=telnet_unknown, terminal_unknown=terminal_unknown
        )
        if match_lines:
            lines.extend(match_lines)

    if lines:
        return "\n".join(lines) + "\n\n"
    return ""


def _color_match(term: "blessed.Terminal", name: str, score: float) -> str:
    """
    Color a nearest-match result by confidence threshold.

    :param score: Similarity as a float 0.0-1.0.
    """
    pct = score * 100
    label = f"{name} ({pct:.0f}%)"
    if term is None:
        return label
    if pct >= 95:
        return str(term.forestgreen(label))
    if pct >= 75:
        return str(term.darkorange(label))
    return str(term.firebrick1(label))


def _nearest_match_lines(
    data: Dict[str, Any],
    names: Dict[str, str],
    term: "blessed.Terminal",
    telnet_unknown: bool = False,
    terminal_unknown: bool = False,
) -> List[str]:
    """Build nearest-match text lines for unknown fingerprints."""
    result_lines: List[str] = []
    if telnet_unknown:
        fp_data = data.get("telnet-probe", {}).get("fingerprint-data")
        if fp_data:
            result = _find_nearest_match(fp_data, "telnet-probe", names)
            if result:
                result_lines.append(f"Nearest telnet match: {_color_match(term, *result)}")
            else:
                result_lines.append("Nearest telnet match: (none)")

    if terminal_unknown:
        fp_data = data.get("terminal-probe", {}).get("fingerprint-data")
        if fp_data:
            result = _find_nearest_match(fp_data, "terminal-probe", names)
            if result:
                result_lines.append(f"Nearest terminal match: {_color_match(term, *result)}")
            else:
                result_lines.append("Nearest terminal match: (none)")
    return result_lines


def _repl_prompt(term: "blessed.Terminal") -> None:
    """Write the REPL prompt with hotkey legend."""
    bk = _bracket_key
    legend = (
        f"{bk(term, 't')}erminal or te{bk(term, 'l')}net details, "  # codespell:ignore te
        f"{bk(term, 's')}ummarize or {bk(term, 'u')}pdate database: "
    )
    echo(f"\r{term.clear_eos}{term.normal}{legend}")


def _paginate(term: "blessed.Terminal", text: str, **_kw: Any) -> None:
    """Display text."""
    for line in text.split("\n"):
        echo(line + "\n")


def _colorize_json(data: Any, term: Optional["blessed.Terminal"] = None) -> str:
    """
    Format JSON with color, preferring bat/batcat over jq.

    :param term: blessed Terminal instance for ``TERM`` kind.
    """
    json_str = json.dumps(data, indent=2, sort_keys=True)
    if _BAT:
        result = subprocess.run(
            [_BAT, "-l", "json", "--style=plain", "--color=always"],
            input=json_str,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.rstrip("\n")
    if _JQ:
        env = {
            "TERM": getattr(term, "kind", None) or "dumb",
            "COLUMNS": str(term.width or 80),
            "LINES": str(term.height or 25),
        }
        if term.number_of_colors == 1 << 24:
            env["COLORTERM"] = "truecolor"
        result = subprocess.run(
            [_JQ, "-C", 'walk(if type=="number" then (.*100|round)/100 else . end)'],
            input=json_str,
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.rstrip("\n")
    return json_str


def _strip_empty_features(d: Dict[str, Any]) -> None:
    """Remove empty kitty/iterm2 feature keys from a dict in-place."""
    for key in list(d):
        if key.startswith(("kitty_", "iterm2_")):
            val = d[key]
            if not val or (isinstance(val, dict) and not any(val.values())):
                del d[key]


def _normalize_color_hex(hex_color: str) -> str:
    """Normalize X11 color hex to standard 6-digit format."""
    from blessed.colorspace import hex_to_rgb, rgb_to_hex

    r, g, b = hex_to_rgb(hex_color)
    return str(rgb_to_hex(r, g, b))


def _filter_terminal_detail(detail: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Filter terminal session data for display."""
    if not detail:
        return detail
    result = dict(detail)

    for key in ("session_arguments", "height", "width"):
        result.pop(key, None)

    aw = result.get("ambiguous_width")
    if aw is not None and aw != 2:
        del result["ambiguous_width"]

    if "text_sizing" in result:
        result["kitty_text_sizing"] = result.pop("text_sizing")

    _strip_empty_features(result)

    terminal_results = result.get("terminal_results")
    if terminal_results is not None:
        terminal_results = dict(terminal_results)
        if "text_sizing" in terminal_results:
            terminal_results["kitty_text_sizing"] = terminal_results.pop("text_sizing")
        for key in ("foreground_color_rgb", "background_color_rgb"):
            terminal_results.pop(key, None)
        _strip_empty_features(terminal_results)
        modes = terminal_results.pop("modes", None)
        if modes:
            dec_modes = {}
            for _num, mode in modes.items():
                if isinstance(mode, dict) and mode.get("supported"):
                    name = mode.get("mode_name", str(_num))
                    dec_modes[name] = {
                        "changeable": mode.get("changeable", False),
                        "enabled": mode.get("enabled", False),
                    }
            if dec_modes:
                terminal_results["dec_private_modes"] = dec_modes
        for key in ("foreground_color_hex", "background_color_hex"):
            if key in terminal_results:
                terminal_results[key] = _normalize_color_hex(terminal_results[key])
        result["terminal_results"] = terminal_results

    test_results = result.get("test_results")
    if test_results is not None:
        filtered = {}
        for k, v in test_results.items():
            if not v:
                continue
            if isinstance(v, dict):
                reduced = {}
                for ver, data in v.items():
                    if isinstance(data, dict):
                        reduced[ver] = {
                            sk: sv for sk, sv in data.items() if sk in ("pct_success", "n_total")
                        }
                    else:
                        reduced[ver] = data
                if reduced:
                    filtered[k] = reduced
            else:
                filtered[k] = v
        if filtered:
            result["test_results"] = filtered
        else:
            del result["test_results"]
    return result


def _filter_telnet_detail(detail: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Filter telnet probe data for display."""
    if not detail:
        return detail
    result = copy.deepcopy(detail)

    if session_data := result.get("session_data"):
        for key in ("probe", "option_states"):
            session_data.pop(key, None)

    if fp_data := result.get("fingerprint-data"):
        fp_data.pop("refused-options", None)

    return result


def _show_detail(term: "blessed.Terminal", data: Dict[str, Any], section: str) -> None:
    """Show detailed JSON for a fingerprint section with pagination."""
    if section == "terminal":
        terminal_probe = data.get("terminal-probe", {})
        detail = _filter_terminal_detail(terminal_probe.get("session_data"))
        title = "Terminal Probe Results"
    else:
        detail = _filter_telnet_detail(data.get("telnet-probe"))
        title = "Telnet Probe Data"

    underline = term.cyan("=" * len(title))
    if detail:
        text = f"{term.magenta(title)}\n" f"{underline}\n" f"\n" f"{_colorize_json(detail, term)}"
        _paginate(term, text)
    else:
        echo(f"{term.magenta(title)}\n{underline}\n\n(no data)\n")


def _client_ip(data: Dict[str, Any]) -> str:
    """Extract client IP from fingerprint data."""
    sessions = data.get("sessions", [])
    if sessions:
        ip = sessions[-1].get("ip")
        if ip:
            return str(ip)
    return "unknown"


def _build_database_entries(
    names: Optional[Dict[str, str]] = None,
) -> List[Tuple[str, str, int, int]]:
    """
    Scan fingerprint directories and build sorted database entries.

    :returns: List of ``(type, display_name, file_count, session_count)`` tuples sorted by session
        count descending.
    """
    client_dir = os.path.join(DATA_DIR, "client") if DATA_DIR else None
    if not client_dir or not os.path.isdir(client_dir):
        return []

    _names = names or {}
    telnet_counts: Dict[str, List[int]] = {}
    terminal_counts: Dict[str, List[int]] = {}
    for telnet_hash in os.listdir(client_dir):
        telnet_path = os.path.join(client_dir, telnet_hash)
        if not os.path.isdir(telnet_path):
            continue
        for terminal_hash in os.listdir(telnet_path):
            terminal_path = os.path.join(telnet_path, terminal_hash)
            if not os.path.isdir(terminal_path):
                continue
            for fname in os.listdir(terminal_path):
                if not fname.endswith(".json"):
                    continue
                n_sessions = 1
                fpath = os.path.join(terminal_path, fname)
                try:
                    with open(fpath, encoding="utf-8") as f:
                        fdata = json.load(f)
                    n_sessions = len(fdata.get("sessions", [1]))
                except (OSError, json.JSONDecodeError):
                    pass
                telnet_counts.setdefault(telnet_hash, [0, 0])
                telnet_counts[telnet_hash][0] += 1
                telnet_counts[telnet_hash][1] += n_sessions
                if terminal_hash != _UNKNOWN_TERMINAL_HASH:
                    terminal_counts.setdefault(terminal_hash, [0, 0])
                    terminal_counts[terminal_hash][0] += 1
                    terminal_counts[terminal_hash][1] += n_sessions

    merged: Dict[Tuple[str, str], List[int]] = {}
    for h, (files, sessions) in telnet_counts.items():
        key = ("Telnet", _resolve_hash_name(h, _names))
        prev = merged.get(key, [0, 0])
        merged[key] = [prev[0] + files, prev[1] + sessions]
    for h, (files, sessions) in terminal_counts.items():
        key = ("Terminal", _resolve_hash_name(h, _names))
        prev = merged.get(key, [0, 0])
        merged[key] = [prev[0] + files, prev[1] + sessions]

    entries = [(kind, name, files, sessions) for (kind, name), (files, sessions) in merged.items()]
    entries.sort(key=lambda e: e[3], reverse=True)
    return entries


def _show_database(
    term: "blessed.Terminal", data: Dict[str, Any], entries: List[Tuple[str, str, int, int]]
) -> None:
    """Display scrollable database of all known fingerprints."""
    try:
        from prettytable import PrettyTable
    except ImportError:
        echo("prettytable not installed.\n")
        return

    if not entries:
        echo("No fingerprints in database.\n")
        return

    has_unicode = _has_unicode(data)

    tbl = PrettyTable()
    if has_unicode:
        _apply_unicode_borders(tbl)
    tbl.title = term.magenta(f"Database ({len(entries)} fingerprints)")
    tbl.field_names = ["Type", "Fingerprint", "Clients", "Calls"]
    tbl.align["Type"] = "l"
    tbl.align["Fingerprint"] = "l"
    tbl.align["Clients"] = "r"
    tbl.align["Calls"] = "r"
    tbl.max_table_width = max(40, (term.width or 80) - 1)
    for kind, display_name, files, sessions in entries:
        tbl.add_row([kind, term.forestgreen(display_name), str(files), str(sessions)])

    _paginate(term, str(tbl))


def _fingerprint_repl(
    term: "blessed.Terminal",
    data: Dict[str, Any],
    seen_counts: str = "",
    filepath: Optional[str] = None,
    names: Optional[Dict[str, str]] = None,
) -> None:
    """Interactive REPL for exploring fingerprint data."""
    ip = _client_ip(data)
    _commands = {
        "q": "logoff",
        "t": "terminal-detail",
        "l": "telnet-detail",
        "s": "database",
        "u": "update",
        "\x0c": "refresh",
    }

    db_cache = None

    while True:
        _repl_prompt(term)
        with term.cbreak():
            while term.inkey(timeout=0):
                pass  # drain pending input (e.g. \r\n after keypress)
            key = term.inkey(timeout=None)

        key_str = key.name or str(key)
        if key_str in _commands:
            echo(str(key) + "\n")
            logger.info("%s: repl %s", ip, _commands[key_str])
        elif key_str not in ("KEY_ENTER", "\r", "\n"):
            logger.info("%s: repl unknown key %r", ip, key_str)

        if key == "q" or key.name == "KEY_ESCAPE" or not key:
            logger.info("%s: repl logoff", ip)
            echo(f"\n{term.normal}")
            break
        if key == "t":
            _show_detail(term, data, "terminal")
        elif key == "l":
            _show_detail(term, data, "telnet")
        elif key == "s":
            if db_cache is None:
                db_cache = _build_database_entries(names)
            _show_database(term, data, db_cache)
        elif key == "u" and filepath is not None:
            _names = names if names is not None else {}
            _prompt_fingerprint_identification(term, data, filepath, _names)
            names = _load_fingerprint_names()
            seen_counts = _build_seen_counts(data, names, term)
        elif key == "\x0c":
            echo(term.normal + term.clear)
            _display_compact_summary(data, term)
            if seen_counts:
                echo(seen_counts)


def _has_unknown_hashes(data: Dict[str, Any], names: Dict[str, str]) -> bool:
    """Return True if either telnet or terminal hash is not yet named."""
    telnet_hash = data.get("telnet-probe", {}).get("fingerprint", "")
    terminal_hash = data.get("terminal-probe", {}).get("fingerprint", _UNKNOWN_TERMINAL_HASH)
    if telnet_hash not in names:
        return True
    if terminal_hash != _UNKNOWN_TERMINAL_HASH and terminal_hash not in names:
        return True
    return False


def _prompt_fingerprint_identification(
    term: "blessed.Terminal", data: Dict[str, Any], filepath: str, names: Dict[str, str]
) -> None:
    """Prompt user to identify unknown fingerprint hashes."""
    telnet_probe = data.get("telnet-probe", {})
    telnet_hash = telnet_probe.get("fingerprint", "")
    terminal_probe = data.get("terminal-probe", {})
    terminal_hash = terminal_probe.get("fingerprint", _UNKNOWN_TERMINAL_HASH)

    telnet_known = telnet_hash in names
    terminal_known = terminal_hash in names or terminal_hash == _UNKNOWN_TERMINAL_HASH
    all_known = telnet_known and terminal_known

    if all_known:
        echo(f"\n{term.bold_magenta}Suggest a revision{term.normal}\n")
    else:
        echo(f"{term.bold_magenta}Help our database!{term.normal}\n")

    suggestions: Dict[str, str] = data.get("suggestions", {})
    revised = False

    if terminal_hash != _UNKNOWN_TERMINAL_HASH:
        if not terminal_known:
            software_name = terminal_probe.get("session_data", {}).get("software_name")
            default = software_name or ""
            if default:
                prompt = f"Terminal emulator name" f' (press return for "{default}"): '
            else:
                prompt = f"Terminal emulator name for {terminal_hash}: "
            raw = _cooked_input(prompt)
            if not raw and default:
                raw = default
            validated = _validate_suggestion(raw)
            if validated:
                suggestions["terminal-emulator"] = validated
        elif all_known:
            current_name = names.get(terminal_hash)
            prompt = f"Terminal emulator name" f' (press return for "{current_name}"): '
            raw = _cooked_input(prompt).strip()
            validated = _validate_suggestion(raw) if raw else None
            if validated and validated != current_name:
                suggestions["terminal-emulator-revision"] = validated
                revised = True

    if not telnet_known:
        raw = _cooked_input(f"Telnet client name for {telnet_hash}: ")
        validated = _validate_suggestion(raw)
        if validated:
            suggestions["telnet-client"] = validated
    elif all_known:
        current_name = names.get(telnet_hash)
        prompt = f"Telnet client name" f' (press return for "{current_name}"): '
        raw = _cooked_input(prompt).strip()
        validated = _validate_suggestion(raw) if raw else None
        if validated and validated != current_name:
            suggestions["telnet-client-revision"] = validated
            revised = True

    if suggestions:
        data["suggestions"] = suggestions
        _atomic_json_write(filepath, data)

    if revised:
        echo("Your submission is under review.\n")

    echo("\n")


def _client_requires_ga(data: Dict[str, Any]) -> bool:
    """Return True when the client refused SGA (e.g. MUD clients like Mudlet)."""
    probe = data.get("telnet-probe", {}).get("session_data", {}).get("probe", {})
    return "SGA" not in probe.get("WILL", {})


def _process_client_fingerprint(filepath: str, data: Dict[str, Any]) -> None:
    """Process client fingerprint: run ucs-detect if available, update file."""
    if _client_requires_ga(data):
        logger.info("skipping ucs-detect: client requires GA (MUD client)")
        terminal_data = None
    else:
        terminal_data = _run_ucs_detect()

    if terminal_data:
        terminal_fp = _create_terminal_fingerprint(terminal_data)
        terminal_hash = _hash_fingerprint(terminal_fp)

        data["terminal-probe"] = {
            "fingerprint": terminal_hash,
            "fingerprint-data": terminal_fp,
            "session_data": terminal_data,
        }

        old_dir = os.path.dirname(filepath)
        if os.path.basename(old_dir) != terminal_hash:
            new_dir = os.path.join(os.path.dirname(old_dir), terminal_hash)
            try:
                os.makedirs(new_dir, exist_ok=True)
                new_filepath = os.path.join(new_dir, os.path.basename(filepath))
                os.rename(filepath, new_filepath)
                filepath = new_filepath
                if not os.listdir(old_dir):
                    os.rmdir(old_dir)
            except OSError as exc:
                logger.warning("failed to move %s -> %s: %s", filepath, new_dir, exc)

        _atomic_json_write(filepath, data)

    _setup_term_environ(data)

    try:
        import blessed  # noqa: F401
    except ImportError:
        print(json.dumps(data, indent=2, sort_keys=True))
        return

    term = _make_terminal()
    names = _load_fingerprint_names()
    seen_counts = _build_seen_counts(data, names, term)
    if not _display_compact_summary(data, term):
        print(json.dumps(data, indent=2, sort_keys=True))
    if seen_counts:
        echo(seen_counts)

    if term.is_a_tty:
        with term.cbreak(), _disable_isig():
            if _has_unknown_hashes(data, names):
                _prompt_fingerprint_identification(term, data, filepath, names)
            _fingerprint_repl(term, data, seen_counts, filepath, names)


def fingerprinting_post_script(filepath: str) -> None:
    """
    Post-fingerprint script that optionally runs ucs-detect for terminal probing.

    If ucs-detect is available in PATH, runs it to collect terminal capabilities
    and merges the results into the fingerprint data.

    Can be used as the TELNETLIB3_FINGERPRINT_POST_SCRIPT target::

        export TELNETLIB3_FINGERPRINT_POST_SCRIPT=telnetlib3.fingerprinting_display
        export TELNETLIB3_DATA_DIR=./data
        telnetlib3-server --shell telnetlib3.fingerprinting_server_shell

    :param filepath: Path to the saved fingerprint JSON file.
    """
    filepath = str(filepath)
    if not os.path.exists(filepath):
        logger.warning("Post-script file not found: %s", filepath)
        return

    with open(filepath, encoding="utf-8") as f:
        data = json.load(f)

    telnet_probe = data.get("telnet-probe", {})
    probed_protocol = telnet_probe.get("fingerprint-data", {}).get("probed-protocol")

    if probed_protocol == "client":
        _process_client_fingerprint(filepath, data)
    else:
        logger.warning("Unknown probed-protocol: %s", probed_protocol)


def main() -> None:
    """CLI entry point for fingerprinting display post-processing."""
    if len(sys.argv) != 2:
        print(f"Usage: python -m {__name__} <filepath>", file=sys.stderr)
        sys.exit(1)
    fingerprinting_post_script(sys.argv[1])


if __name__ == "__main__":  # pragma: no cover
    main()
