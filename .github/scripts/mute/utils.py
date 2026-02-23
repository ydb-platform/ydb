"""Mute package utilities: parse mute file, etc."""

import os


def parse_mute_file(path):
    """Parse mute file, return set of non-empty, non-comment lines."""
    if path is None or path == '':
        return set()
    if not os.path.exists(path):
        return set()
    with open(path) as f:
        return set(
            l.strip()
            for l in f
            if l.strip() and not l.strip().startswith("#")
        )
