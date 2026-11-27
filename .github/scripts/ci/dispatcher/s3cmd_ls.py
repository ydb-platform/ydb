"""Parse one `s3cmd ls` output line.

Object keys may contain spaces (the host workflow name is ``CI remote executor``).
The URI is the substring from ``s3://`` through the end of the line, not the last
whitespace-separated token. The host workflow inlines this function; keep them
in sync (see REMOTE_PULL.md).
"""
from __future__ import annotations


def parse_s3cmd_ls_line(line: str) -> str | None:
    """Return the s3:// URI from an s3cmd ls line, or None if it is not one."""
    text = line.strip()
    marker = "s3://"
    idx = text.find(marker)
    if idx < 0:
        return None
    if idx > 0 and not text[idx - 1].isspace():
        return None
    uri = text[idx:]
    if len(uri) <= len(marker):
        return None
    return uri
