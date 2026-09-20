"""GFM autolink extension rules.

Three inline scanners are registered:

- **gfm_autolink_www** (char ``w``): bare ``www.`` URLs.
  Uses ``add_terminator_char("w")`` so the text scanner interrupts at ``w``.
- **gfm_autolink_protocol** (char ``:``): ``http://``, ``https://``,
  ``mailto:``, ``xmpp:`` URLs via back-scanning ``pending``.
- **gfm_autolink_email** (char ``@``): bare email addresses via
  back-scanning ``pending``.

Since ``:`` and ``@`` are already default terminator characters in
markdown-it-py, the protocol and email rules are invoked at every occurrence
of those characters. They use a *back-scanning* approach: looking backwards
through ``state.pending`` for a protocol prefix or email local-part that was
accumulated by the text rule. This means every ``:`` and ``@`` in the
document incurs a (cheap) regex check or character scan of pending text.

The trade-off vs. a **core-rule** (post-processing) approach — which would
walk the final token stream, find autolink patterns in text tokens, and
split them — is:

- **Inline approach** (current): simpler, integrates naturally with
  ``state.linkLevel`` to suppress matching inside links, but relies on the
  prefix being present in ``state.pending`` (if a prior inline rule consumed
  part of the prefix, matching would fail — unlikely in practice).
- **Core-rule approach**: guaranteed to find all autolinks regardless of
  inline rule ordering, but requires token-stream surgery (splitting text
  tokens and inserting link tokens) and cannot easily interact with nesting
  guards like ``linkLevel``.

The ``w`` terminator is the only *new* terminator added. It causes the text
rule to interrupt at every ``w``, which is a minor performance cost for
documents heavy in that letter, but necessary since ``www.`` must be matched
from the start of the URL.

Specification: https://github.github.com/gfm/#autolinks-extension-

.. versionadded:: 0.5.0

Requires markdown-it-py ≥ 4.1.0.
"""

from __future__ import annotations

import re

from markdown_it import MarkdownIt
from markdown_it.rules_inline import StateInline

from ._match import check_prev, match_any_email, match_http, match_www

# Regex to back-scan pending text for a protocol name ending at the current
# position (the colon character).
_PROTO_RE = re.compile(r"(?:^|.)(https?|mailto|xmpp)$", re.DOTALL)


def gfm_autolink_plugin(md: MarkdownIt) -> None:
    """Enable the GFM autolink extension.

    Recognises bare ``www.`` URLs, ``http(s)://`` URLs,
    ``mailto:``/``xmpp:`` links, and bare email addresses.

    Requires markdown-it-py ≥ 4.1.0.
    """
    if not hasattr(md.inline, "add_terminator_char"):
        raise RuntimeError("gfm_autolink_plugin requires markdown-it-py >= 4.1.0")

    md.inline.add_terminator_char("w")
    md.inline.ruler.push("gfm_autolink_www", _www_inline_rule)
    md.inline.ruler.push("gfm_autolink_protocol", _protocol_rule)
    md.inline.ruler.push("gfm_autolink_email", _email_rule)


# ---------------------------------------------------------------------------
# Helpers (inline rules)
# ---------------------------------------------------------------------------


def _preceding_ok(state: StateInline, bscan_len: int) -> bool:
    """Check whether the character before the back-scanned portion allows an autolink."""
    abs_pos = state.pos - bscan_len
    if abs_pos <= 0:
        return True
    preceding = state.src[abs_pos - 1]
    return check_prev(preceding)


def _create_autolink(
    state: StateInline,
    bscan_len: int,
    total_len: int,
    url: str,
    text: str,
) -> bool:
    """Emit ``link_open`` / ``text`` / ``link_close`` tokens.

    *bscan_len* characters are trimmed from the end of ``state.pending``
    (the back-scanned protocol or local part).  The parser position is
    then advanced by ``total_len - bscan_len`` characters.
    """
    if bscan_len:
        state.pending = state.pending[:-bscan_len]

    full_url = state.md.normalizeLink(url)
    if not state.md.validateLink(full_url):
        return False

    token = state.push("link_open", "a", 1)
    token.attrs = {"href": full_url}
    token.markup = "autolink"
    token.info = "auto"

    token = state.push("text", "", 0)
    token.content = state.md.normalizeLinkText(text)

    token = state.push("link_close", "a", -1)
    token.markup = "autolink"
    token.info = "auto"

    state.pos += total_len - bscan_len
    return True


# ---------------------------------------------------------------------------
# www inline rule  (requires add_terminator_char("w") — markdown-it-py >= 4.1.0)
# ---------------------------------------------------------------------------


def _www_inline_rule(state: StateInline, silent: bool) -> bool:
    """Match ``www.`` autolinks as an inline rule (trigger char: ``w``)."""
    if state.linkLevel > 0:
        return False

    pos = state.pos
    src = state.src

    # Quick check: must be 'w' and form "www."
    if src[pos] != "w":
        return False
    if pos + 4 > state.posMax or src[pos : pos + 4] != "www.":
        return False

    # Check preceding character (from pending text or start-of-line).
    if state.pending:
        preceding = state.pending[-1]
        if not check_prev(preceding):
            return False
    elif pos > 0:
        preceding = src[pos - 1]
        if not check_prev(preceding):
            return False

    result = match_www(src[pos : state.posMax])
    if result is None:
        return False

    url, length = result
    label = src[pos : pos + length]

    if silent:
        return True

    full_url = state.md.normalizeLink(url)
    if not state.md.validateLink(full_url):
        return False

    token = state.push("link_open", "a", 1)
    token.attrs = {"href": full_url}
    token.markup = "autolink"
    token.info = "auto"

    token = state.push("text", "", 0)
    token.content = state.md.normalizeLinkText(label)

    token = state.push("link_close", "a", -1)
    token.markup = "autolink"
    token.info = "auto"

    state.pos += length
    return True


# ---------------------------------------------------------------------------
# Protocol scanner  (trigger char: ':')
# ---------------------------------------------------------------------------


def _protocol_rule(state: StateInline, silent: bool) -> bool:
    if state.linkLevel > 0:
        return False

    pos = state.pos
    remaining = state.src[pos : state.posMax]

    # Must start with ':' and have at least 3 more characters.
    if len(remaining) < 4 or remaining[0] != ":":
        return False

    # Back-scan pending text for a known protocol name.
    m = _PROTO_RE.search(state.pending)
    if m is None:
        return False

    proto = m.group(1)
    bscan_len = len(proto)

    if not _preceding_ok(state, bscan_len):
        return False

    # Combine back-scanned protocol with the remaining text.
    combined = proto + remaining

    if proto in ("mailto", "xmpp"):
        result = match_any_email(combined, bscan_len + 1, proto)
    else:
        result = match_http(combined)

    if result is None:
        return False

    full_url, total_len = result
    label = combined[:total_len]

    if silent:
        return True
    return _create_autolink(state, bscan_len, total_len, full_url, label)


# ---------------------------------------------------------------------------
# Bare email scanner  (trigger char: '@')
# ---------------------------------------------------------------------------


def _email_rule(state: StateInline, silent: bool) -> bool:
    if state.linkLevel > 0:
        return False

    pos = state.pos
    if pos >= state.posMax or state.src[pos] != "@":
        return False
    # Need at least one character after '@'.
    if pos + 1 >= state.posMax:
        return False

    # Back-scan pending text for the local part of the email.
    local_rev: list[str] = []
    for ch in reversed(state.pending):
        if ch.isascii() and (ch.isalnum() or ch in ".+-_"):
            local_rev.append(ch)
        else:
            break

    if not local_rev:
        return False

    local_len = len(local_rev)
    if not _preceding_ok(state, local_len):
        return False

    # Forward-scan for the domain part.
    after_at = state.src[pos + 1 : state.posMax]
    domain_len = 0
    num_period = 0
    for i, ch in enumerate(after_at):
        if ch.isascii() and ch.isalnum():
            pass
        elif ch == "@":
            return False
        elif (
            ch == "."
            and i + 1 < len(after_at)
            and after_at[i + 1].isascii()
            and after_at[i + 1].isalnum()
        ):
            num_period += 1
        elif ch != "-" and ch != "_":
            break
        domain_len += 1

    if domain_len == 0 or num_period == 0:
        return False

    last_ch = after_at[domain_len - 1]
    if not (last_ch.isascii() and last_ch.isalnum()) and last_ch != ".":
        return False

    local_part = "".join(reversed(local_rev))
    email_text = local_part + state.src[pos : pos + 1 + domain_len]
    total_len = local_len + 1 + domain_len
    url = "mailto:" + email_text

    if silent:
        return True
    return _create_autolink(state, local_len, total_len, url, email_text)
