"""URL / email matching helpers for the GFM autolink extension.

Ported from the Rust ``gfm_autolinks`` crate.
"""

from __future__ import annotations

import unicodedata

# ---------------------------------------------------------------------------
# Character classification helpers
# ---------------------------------------------------------------------------

_VALID_PREV_CHARS = frozenset(" \t\r\n*_~(")


def check_prev(ch: str) -> bool:
    """Return ``True`` if *ch* is a valid preceding character for an autolink."""
    return ch in _VALID_PREV_CHARS


def _is_valid_hostchar(ch: str) -> bool:
    """Return ``True`` if *ch* is valid inside a domain label (not whitespace/punctuation)."""
    if ch.isspace():
        return False
    cat = unicodedata.category(ch)
    # Unicode punctuation categories: Pc, Pd, Pe, Pf, Pi, Po, Ps
    return not cat.startswith("P")


# Characters that terminate a URL (before autolink_delim trimming).
_SPACE_CHARS = frozenset(" \t\r\n\x00\x0b\x0c")


def _isspace(ch: str) -> bool:
    return ch in _SPACE_CHARS


_LINK_END_ASSORTMENT = frozenset("?!.,:*_~'\"[]")


def _autolink_delim(data: str, link_end: int) -> int:
    """Trim trailing punctuation from a URL according to GFM rules."""
    # Truncate at first '<'
    for i, ch in enumerate(data[:link_end]):
        if ch == "<":
            link_end = i
            break

    while link_end > 0:
        cclose = data[link_end - 1]

        copen = "(" if cclose == ")" else None

        if cclose in _LINK_END_ASSORTMENT:
            link_end -= 1
        elif cclose == ";":
            new_end = link_end - 2
            while new_end > 0 and data[new_end].isalpha():
                new_end -= 1
            if new_end < link_end - 2 and data[new_end] == "&":
                link_end = new_end
            else:
                link_end -= 1
        elif copen is not None:
            opening = data[:link_end].count(copen)
            closing = data[:link_end].count(cclose)
            if closing <= opening:
                break
            link_end -= 1
        else:
            break

    return link_end


# ---------------------------------------------------------------------------
# Domain validation
# ---------------------------------------------------------------------------


def _check_domain(data: str, allow_short: bool) -> int | None:
    """Validate a domain name and return the length consumed, or ``None``."""
    if not data:
        return None

    np = 0
    uscore1 = 0
    uscore2 = 0

    for i, ch in enumerate(data):
        if ch == "_":
            uscore2 += 1
        elif ch == ".":
            uscore1 = uscore2
            uscore2 = 0
            np += 1
        elif not _is_valid_hostchar(ch) and ch != "-":
            if uscore1 == 0 and uscore2 == 0 and (allow_short or np > 0):
                return i
            return None
        # else: valid hostchar or '-'

    if (uscore1 > 0 or uscore2 > 0) and np <= 10:
        return None
    if allow_short or np > 0:
        return len(data)
    return None


# ---------------------------------------------------------------------------
# www matching
# ---------------------------------------------------------------------------

_EMAIL_OK = frozenset(".+-_")


def match_www(text: str) -> tuple[str, int] | None:
    """Match a bare ``www.`` URL at the start of *text*.

    Returns ``(url_with_scheme, char_count)`` or ``None``.
    """
    if not text.startswith("www."):
        return None

    link_end = _check_domain(text[4:], False)
    if link_end is None:
        return None
    # link_end is offset from position 4
    link_end += 4

    # extend to the end of non-space characters
    while link_end < len(text) and not _isspace(text[link_end]):
        link_end += 1

    link_end = _autolink_delim(text, link_end)

    matched = text[:link_end]
    url = "http://" + matched
    return url, len(matched)


# ---------------------------------------------------------------------------
# http(s):// matching
# ---------------------------------------------------------------------------


def match_http(text: str) -> tuple[str, int] | None:
    """Match an ``http://`` or ``https://`` URL at the start of *text*.

    Returns ``(url, char_count)`` or ``None``.
    """
    if text.startswith("http://"):
        prefix_len = 7
    elif text.startswith("https://"):
        prefix_len = 8
    else:
        return None

    link_end = _check_domain(text[prefix_len:], True)
    if link_end is None:
        return None
    link_end += prefix_len

    while link_end < len(text) and not _isspace(text[link_end]):
        link_end += 1

    link_end = _autolink_delim(text, link_end)

    url = text[:link_end]
    return url, len(url)


# ---------------------------------------------------------------------------
# Email matching
# ---------------------------------------------------------------------------


def match_email(text: str) -> tuple[str, int] | None:
    """Match an email address (optionally prefixed by ``mailto:``/``xmpp:``)."""
    pos = 0
    protocol: str | None = None
    if text.startswith("mailto:"):
        protocol = "mailto"
        pos = 7
    elif text.startswith("xmpp:"):
        protocol = "xmpp"
        pos = 5

    return match_any_email(text, pos, protocol)


def match_any_email(
    text: str, pos: int, protocol: str | None
) -> tuple[str, int] | None:
    """Match an email address in *text* starting the local-part scan at *pos*.

    *protocol* is ``"mailto"``, ``"xmpp"``, or ``None`` (bare address).
    Returns ``(url, char_count)`` or ``None``.
    """
    size = len(text)

    # scan local part (before @)
    start_pos = pos
    while pos < size:
        ch = text[pos]
        if ch.isascii() and (ch.isalnum() or ch in _EMAIL_OK):
            pos += 1
            continue
        if ch == "@":
            break
        return None

    if pos == start_pos:
        return None

    # scan domain (after @)
    link_end = pos + 1
    np = 0
    num_slash = 0

    while link_end < size:
        ch = text[link_end]
        if ch.isascii() and ch.isalnum():
            pass
        elif ch == "@":
            if protocol != "xmpp":
                return None
        elif (
            ch == "."
            and link_end < size - 1
            and text[link_end + 1].isascii()
            and text[link_end + 1].isalnum()
        ):
            np += 1
        elif ch == "/" and protocol == "xmpp" and num_slash == 0:
            num_slash += 1
        elif ch != "-" and ch != "_":
            break
        link_end += 1

    if link_end < 2 or np == 0:
        return None
    last_ch = text[link_end - 1]
    if not (last_ch.isascii() and last_ch.isalpha()) and last_ch != ".":
        return None

    url = "mailto:" + text[:link_end] if protocol is None else text[:link_end]

    return url, link_end
