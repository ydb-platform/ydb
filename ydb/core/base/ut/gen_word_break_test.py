#!/usr/bin/env python3
"""
Generates word_break_test.json from Unicode WordBreakTest.txt.

Parses WordBreakTest.txt to extract test sequences with break/no-break
markers, then uses WordBreakProperty.txt to identify which segments
contain "wanted" characters (ALetter, Hebrew_Letter, Katakana, Numeric).
Lines where no segment contains wanted characters are skipped.

Output format:
[
  {"input": "<full string>", "tokens": ["<token1>", "<token2>", ...]},
  ...
]
"""

import json
import re
import urllib.request
from pathlib import Path

VERSION = "12.1.0"
URL_PREFIX = f"http://www.unicode.org/Public/{VERSION}/ucd"

WORD_BREAK_TEST_URL = f"{URL_PREFIX}/auxiliary/WordBreakTest.txt"
WORD_BREAK_PROP_URL = f"{URL_PREFIX}/auxiliary/WordBreakProperty.txt"

WORD_BREAK_TEST_FILE = f"WordBreakTest-{VERSION}.txt"
WORD_BREAK_PROP_FILE = f"WordBreakProperty-{VERSION}.txt"

OUTPUT_FILE = "word_break_test.json"

# Only keep segments that contain at least one character with these properties
WANTED_PROPERTIES = {"aletter", "hebrew_letter", "katakana", "numeric"}


def download_if_missing(url, filename):
    if not Path(filename).is_file():
        print(f"Downloading {url}...")
        urllib.request.urlretrieve(url, filename)
        print("done.")


def parse_word_break_properties(filename):
    """Parse WordBreakProperty.txt and return set of codepoints with wanted properties."""
    wanted = set()
    with open(filename, "r", encoding="utf-8") as f:
        for line in f:
            line = re.sub(r"#.*", "", line).strip()
            if not line:
                continue
            m = re.match(
                r"([0-9A-F]{4,6})(?:\.\.([0-9A-F]{4,6}))?\s*;\s*(\S+)",
                line,
                re.IGNORECASE,
            )
            if m:
                start = int(m.group(1), 16)
                end = int(m.group(2), 16) if m.group(2) else start
                prop = m.group(3).strip().lower()
                if prop in WANTED_PROPERTIES:
                    for cp in range(start, end + 1):
                        wanted.add(cp)
    return wanted


def codepoints_to_str(codepoints):
    """Convert a list of integer codepoints to a Python string."""
    return "".join(chr(cp) for cp in codepoints)


def main():
    download_if_missing(WORD_BREAK_TEST_URL, WORD_BREAK_TEST_FILE)
    download_if_missing(WORD_BREAK_PROP_URL, WORD_BREAK_PROP_FILE)

    wanted_codepoints = parse_word_break_properties(WORD_BREAK_PROP_FILE)
    print(f"Loaded {len(wanted_codepoints)} wanted codepoints from {WORD_BREAK_PROP_FILE}")

    results = []

    with open(WORD_BREAK_TEST_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Extract sequence part (before the comment)
            m = re.match(r"^(.*?)\s*#", line)
            if not m:
                continue
            sequence = m.group(1).strip()

            # Remove leading and trailing ÷
            sequence = re.sub(r"^\s*÷\s*", "", sequence)
            sequence = re.sub(r"\s*÷\s*$", "", sequence)

            if not sequence:
                continue

            # Split by ÷ to get segments (potential tokens)
            segments = re.split(r"\s*÷\s*", sequence)

            # Build full input string from all codepoints
            all_codepoints = []
            for seg in segments:
                for h in re.findall(r"[0-9A-Fa-f]+", seg):
                    all_codepoints.append(int(h, 16))
            input_str = codepoints_to_str(all_codepoints)

            # Extract tokens: segments containing at least one wanted codepoint
            tokens = []
            for seg in segments:
                seg_codepoints = [int(h, 16) for h in re.findall(r"[0-9A-Fa-f]+", seg)]
                if any(cp in wanted_codepoints for cp in seg_codepoints):
                    tokens.append(codepoints_to_str(seg_codepoints))

            # Skip lines without any wanted tokens
            if not tokens:
                continue

            results.append({"input": input_str, "tokens": tokens})

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Generated {len(results)} test cases to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
