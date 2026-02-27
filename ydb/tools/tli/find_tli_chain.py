#!/usr/bin/env python3
"""
TLI Chain Finder - Extracts TLI (Transaction Lock Invalidation) chain info from logs.

Given a VictimQuerySpanId, finds the related breaker transaction and displays
the query texts for both victim and breaker transactions.
"""

import argparse
import re
import sys
import os
from datetime import datetime
from typing import Optional, Dict, Tuple, List


# ==================== ANSI Styling ====================

ANSI_RED = "\033[31m"
ANSI_CYAN = "\033[36m"
ANSI_BOLD = "\033[1m"
ANSI_RESET = "\033[0m"


def use_color_enabled(no_color_flag: bool) -> bool:
    """Check if ANSI colors should be used."""
    if no_color_flag:
        return False
    if os.getenv("NO_COLOR") is not None:
        return False
    return sys.stdout.isatty()


def style(s: str, *, color: Optional[str] = None, bold: bool = False, enable: bool = True) -> str:
    """Apply ANSI styling to a string."""
    if not enable:
        return s
    parts = []
    if bold:
        parts.append(ANSI_BOLD)
    if color:
        parts.append(color)
    parts.append(s)
    parts.append(ANSI_RESET)
    return "".join(parts)


def color_red(s: str, enable: bool) -> str:
    return style(s, color=ANSI_RED, enable=enable)


def print_section_header(title: str, enable_color: bool):
    """Print a section header with decorative bars."""
    bar = "=" * max(48, len(title) + 8)
    print()
    print(style(bar, color=ANSI_CYAN, bold=True, enable=enable_color))
    print(style(f"  {title}", color=ANSI_CYAN, bold=True, enable=enable_color))
    print(style(bar, color=ANSI_CYAN, bold=True, enable=enable_color))
    print()


def print_kv_header(key: str, enable_color: bool):
    """Print a key-value header."""
    print(style(f"{key}:", color=ANSI_CYAN, bold=True, enable=enable_color), end=" ")


# ==================== Text Helpers ====================

def unescape_and_format_query_text(s: Optional[str]) -> str:
    """Unescape and format query text for display."""
    if not s:
        return ""
    try:
        s2 = s.encode("utf-8").decode("unicode_escape")
    except Exception:
        s2 = s.replace(r"\n", "\n").replace(r"\t", " ").replace(r"\"", "\"")
    s2 = s2.replace("\t", " ")
    s2 = "\n".join(line.rstrip() for line in s2.splitlines()).strip()
    return s2


def extract_between(line: str, field: str, end_field: Optional[str]) -> Optional[str]:
    """Extract value between field marker and optional end marker."""
    key = f"{field}:"
    pos = line.find(key)
    if pos < 0:
        return None
    start = pos + len(key)
    s = line[start:].lstrip()
    if end_field:
        marker = f", {end_field}"
        end_pos = s.find(marker)
        if end_pos >= 0:
            return s[:end_pos]
    return s


# ==================== Regex Patterns ====================

RE_VICTIM_ID = re.compile(r"\bVictimQuerySpanId:\s*(\d+)\b")
RE_BREAKER_ID = re.compile(r"\bBreakerQuerySpanId:\s*(\d+)\b")
RE_VICTIM_IDS_LIST = re.compile(r"\bVictimQuerySpanIds:\s*\[([^\]]*)\]")
RE_ISO = re.compile(r"\b(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?Z\b")
RE_TX_ITEM = re.compile(
    r"QuerySpanId=(\d+)\s+QueryText=(.*?)(?=\s+\|\s+QuerySpanId=|\s*\Z)",
    re.DOTALL
)


# ==================== Timestamp Parsing ====================

class FastTimeParser:
    """Fast ISO timestamp parser with date caching."""

    def __init__(self):
        self._cached_date: Optional[str] = None
        self._cached_day_base: int = 0

    def parse(self, line: str) -> Optional[float]:
        """Parse ISO timestamp from line, return seconds since epoch."""
        m = RE_ISO.search(line)
        if not m:
            return None
        date_s, hh, mm, ss, frac = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
        if date_s != self._cached_date:
            d = datetime.strptime(date_s, "%Y-%m-%d").date()
            self._cached_day_base = d.toordinal() * 86400
            self._cached_date = date_s
        t = self._cached_day_base + int(hh) * 3600 + int(mm) * 60 + int(ss)
        if frac:
            frac = (frac + "000000")[:6]
            t = t + int(frac) / 1_000_000.0
        return float(t)


# ==================== Line Matching Helpers ====================

def extract_breaker_id(line: str) -> Optional[str]:
    """Extract BreakerQuerySpanId from line."""
    m = RE_BREAKER_ID.search(line)
    return m.group(1) if m else None


def victim_id_in_line(line: str, victim_id: str) -> bool:
    """Check if victim_id appears in VictimQuerySpanId or VictimQuerySpanIds."""
    if re.search(rf"\bVictimQuerySpanId:\s*{re.escape(victim_id)}\b", line):
        return True
    m = RE_VICTIM_IDS_LIST.search(line)
    if m:
        return re.search(rf"\b{re.escape(victim_id)}\b", m.group(1)) is not None
    return False


def in_window(t: float, start: float, end: float) -> bool:
    """Check if timestamp is within window."""
    return start <= t <= end


# ==================== Query Text Parsing ====================

def count_queries_in_list(line: str, list_field: str) -> int:
    """Count number of queries in a QueryTexts list field."""
    marker = f"{list_field}: ["
    start = line.find(marker)
    if start < 0:
        return 0
    start += len(marker)
    end = line.rfind("]")
    if end < start:
        return 0
    payload = line[start:end]
    # Count occurrences of "QuerySpanId=" which marks each query
    return payload.count("QuerySpanId=")


def parse_query_texts_list(line: str, list_field: str) -> List[Tuple[str, str]]:
    """Parse QueryTexts list field into (QuerySpanId, QueryText) pairs."""
    marker = f"{list_field}: ["
    start = line.find(marker)
    if start < 0:
        return []
    start += len(marker)
    end = line.rfind("]")
    if end < start:
        return []
    payload = line[start:end].strip()

    out: List[Tuple[str, str]] = []
    for m in RE_TX_ITEM.finditer(payload):
        qid = m.group(1)
        qtext_raw = m.group(2).strip()
        out.append((qid, unescape_and_format_query_text(qtext_raw)))
    return out


def print_tx_block(title: str, items: List[Tuple[str, str]], highlight_id: Optional[str], use_color: bool):
    """Print transaction block with optional highlighting."""
    print_section_header(title, use_color)
    if not items:
        print("(not found)")
        return

    for i, (qid, qtext) in enumerate(items):
        text_to_print = qtext
        if highlight_id and qid == highlight_id:
            text_to_print = color_red(text_to_print, use_color)
        print(text_to_print)
        if i != len(items) - 1:
            print()


# ==================== Main Logic ====================

def main():
    ap = argparse.ArgumentParser(
        description="Find TLI chain by VictimQuerySpanId in logs."
    )
    ap.add_argument("victim_id", help="VictimQuerySpanId (number)")
    ap.add_argument("logfile", help="Path to log file")
    ap.add_argument("--window-sec", type=float, default=10.0, help="Chain time window +/- seconds (default: 10)")
    ap.add_argument("--no-color", action="store_true", help="Disable ANSI colors/styles")
    args = ap.parse_args()

    victim_id = args.victim_id
    path = args.logfile
    W = float(args.window_sec)
    use_color = use_color_enabled(args.no_color)

    tp = FastTimeParser()

    # Collected data
    anchor_t: Optional[float] = None
    victim_log_sa: Optional[str] = None      # SessionActor "was a victim" line
    breaker_log_ds: Optional[str] = None     # DataShard "broke other locks" line
    breaker_id: Optional[str] = None
    breaker_sa_by_id: Dict[str, str] = {}
    breaker_sa_with_text_by_id: Dict[str, str] = {}

    # First pass: find anchor time and collect all relevant lines
    # Since logs may be unsorted, we need to scan the entire file
    relevant_lines: List[Tuple[float, str]] = []

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            # Quick filter to skip irrelevant lines
            if not (
                (victim_id in line) or
                ("was a victim of broken locks" in line) or
                ("broke other locks" in line) or
                ("had broken other locks" in line)
            ):
                continue

            t = tp.parse(line)
            if t is None:
                continue

            # Find anchor: first line containing victim_id
            if anchor_t is None and (victim_id in line):
                anchor_t = t

            relevant_lines.append((t, line))

    if anchor_t is None:
        print(f"Error: VictimQuerySpanId {victim_id} not found in log file.", file=sys.stderr)
        sys.exit(1)

    w_start = anchor_t - W
    w_end = anchor_t + W

    # Process all relevant lines within the time window
    for t, line in relevant_lines:
        if not in_window(t, w_start, w_end):
            continue

        # Victim SessionActor line: "was a victim of broken locks" + Component: SessionActor
        if victim_log_sa is None:
            if ("was a victim of broken locks" in line) and \
               ("Component: SessionActor" in line) and \
               victim_id_in_line(line, victim_id):
                victim_log_sa = line.rstrip("\n")

        # Breaker DataShard line: "broke other locks" + Component: DataShard
        if breaker_log_ds is None:
            if ("broke other locks" in line) and \
               ("Component: DataShard" in line or "datashard_integrity_trails" in line) and \
               victim_id_in_line(line, victim_id):
                breaker_log_ds = line.rstrip("\n")
                breaker_id = extract_breaker_id(line)

        # Breaker SessionActor lines: "had broken other locks" + Component: SessionActor
        # Keep the line with the most queries in BreakerQueryTexts (prefer Commit over deferred)
        if ("had broken other locks" in line) and ("Component: SessionActor" in line):
            bid = extract_breaker_id(line)
            if bid:
                sline = line.rstrip("\n")
                new_count = count_queries_in_list(sline, "BreakerQueryTexts")
                # Update if this is the first line or has more queries
                if bid not in breaker_sa_by_id:
                    breaker_sa_by_id[bid] = sline
                else:
                    old_count = count_queries_in_list(breaker_sa_by_id[bid], "BreakerQueryTexts")
                    if new_count > old_count:
                        breaker_sa_by_id[bid] = sline
                # Same logic for lines with BreakerQueryText field
                if "BreakerQueryText:" in sline:
                    if bid not in breaker_sa_with_text_by_id:
                        breaker_sa_with_text_by_id[bid] = sline
                    else:
                        old_count = count_queries_in_list(breaker_sa_with_text_by_id[bid], "BreakerQueryTexts")
                        if new_count > old_count:
                            breaker_sa_with_text_by_id[bid] = sline

    # Extract query texts
    victim_query_text = ""
    if victim_log_sa:
        vqt = extract_between(victim_log_sa, "VictimQueryText", "VictimQueryTexts:")
        victim_query_text = unescape_and_format_query_text(vqt or "")

    breaker_log_sa: Optional[str] = None
    breaker_query_text = ""
    if breaker_id:
        breaker_log_sa = breaker_sa_with_text_by_id.get(breaker_id) or breaker_sa_by_id.get(breaker_id)
        if breaker_log_sa:
            bqt = extract_between(breaker_log_sa, "BreakerQueryText", "BreakerQueryTexts:")
            breaker_query_text = unescape_and_format_query_text(bqt or "")

    # Output results
    print_section_header("TLI Chain", use_color)

    print_kv_header("VictimQuerySpanId", use_color)
    print(victim_id)
    print()

    print_kv_header("VictimQueryText", use_color)
    print(victim_query_text if victim_query_text else "(not found)")
    print("\n")

    print_kv_header("BreakerQuerySpanId", use_color)
    print(breaker_id if breaker_id else "(not found)")
    print()

    print_kv_header("BreakerQueryText", use_color)
    print(breaker_query_text if breaker_query_text else "(not found)")

    # Parse and display transaction blocks
    victim_tx_items: List[Tuple[str, str]] = []
    if victim_log_sa:
        victim_tx_items = parse_query_texts_list(victim_log_sa, "VictimQueryTexts")

    breaker_tx_items: List[Tuple[str, str]] = []
    if breaker_log_sa:
        breaker_tx_items = parse_query_texts_list(breaker_log_sa, "BreakerQueryTexts")

    print_tx_block("VictimTx", victim_tx_items, victim_id, use_color)
    print_tx_block("BreakerTx", breaker_tx_items, breaker_id, use_color)


if __name__ == "__main__":
    main()
