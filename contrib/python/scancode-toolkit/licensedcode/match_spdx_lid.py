# -*- coding: utf-8 -*-
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import os
import re
import sys

from license_expression import Keyword
from license_expression import LicenseSymbol
from license_expression import LicenseWithExceptionSymbol
from license_expression import Licensing

from licensedcode.match import LicenseMatch
from licensedcode.models import SpdxRule
from licensedcode.spans import Span

"""
Matching strategy for license expressions and "SPDX-License-Identifier:"
expression tags.
The matching aproach is a tad different:

First, we do not run this matcher against whoel queries. Instead the matchable
text is collected during the query processing as Query.spdx_lines for any line
that starts withs these tokens ['spdx', 'license', 'identifier'] or ['spdx',
'licence', 'identifier'] begining with the first, second or third token position
in a line.

Then the words after "SPDX-license-identifier" are parsed as if theyr were an
SPDX license expression (with a few extra symbols and/or deprecated symbols
added to the list of license keys.
"""

# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE or os.environ.get('SCANCODE_DEBUG_LICENSE'):
    import logging

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


MATCH_SPDX_ID = '1-spdx-id'


def spdx_id_match(idx, query_run, text):
    """
    Return one LicenseMatch by matching the `text` as an SPDX license expression
    using the `query_run` positions and `idx` index for support.
    """
    from licensedcode.cache import get_spdx_symbols
    from licensedcode.cache import get_unknown_spdx_symbol

    if TRACE:
        logger_debug('spdx_id_match: start:', 'text:', text, 'query_run:', query_run)

    licensing = Licensing()
    symbols_by_spdx = get_spdx_symbols()
    unknown_symbol = get_unknown_spdx_symbol()

    _prefix, exp_text = prepare_text(text)
    expression = get_expression(exp_text, licensing, symbols_by_spdx, unknown_symbol)
    if expression is None:
        return
    expression_str = expression.render()

    match_len = len(query_run)
    match_start = query_run.start
    matched_tokens = query_run.tokens

    # build synthetic rule
    # TODO: ensure that all the SPDX license keys are known symbols
    rule = SpdxRule(
        license_expression=expression_str,
        # FIXME: for now we are putting the original query text as a
        # rule text: this is likely incorrect when it comes to properly
        # computing the known and unknowns and high and lows for this rule.
        # Alternatively we could use the expression string, padded with
        # spdx-license-identifier: this may be wrong too, if the line was
        # not padded originally with this tag
        stored_text=text,
        length=match_len,
    )

    # build match from parsed expression
    # collect match start and end: e.g. the whole text
    qspan = Span(range(match_start, query_run.end + 1))

    # we use the query side to build the ispans
    ispan = Span(range(0, match_len))

    len_legalese = idx.len_legalese
    hispan = Span(p for p, t in enumerate(matched_tokens) if t < len_legalese)

    match = LicenseMatch(
        rule=rule,
        qspan=qspan,
        ispan=ispan,
        hispan=hispan,
        query_run_start=match_start,
        matcher=MATCH_SPDX_ID,
        query=query_run.query,
    )
    return match


def get_expression(text, licensing, spdx_symbols, unknown_symbol):
    """
    Return an Expression object by parsing the `text` string using
    the `licensing` reference Licensing.

    Note that an expression is ALWAYS returned: if the parsing fails or some
    other error happens somehow, this function returns instead a bare
    expression made of only "unknown-spdx" symbol.
    """
    _prefix, text = prepare_text(text)
    if not text:
        return
    expression = None
    try:
        expression = _parse_expression(text, licensing, spdx_symbols, unknown_symbol)
    except Exception:
        try:
            # try to parse again using a lenient recovering parsing process
            # such as for plain space or comma-separated list of licenses (e.g. UBoot)
            expression = _reparse_invalid_expression(text, licensing, spdx_symbols, unknown_symbol)
        except Exception:
            pass

    if expression is None:
        expression = unknown_symbol
    return expression


# TODO: use me??: this is NOT used at all for now because too complex for a too
# small benefit: only ecos-2.0 has ever been see in the wild in U-Boot
# identifiers
# Some older SPDX ids are deprecated and therefore no longer referenced in
# licenses so we track them here. This maps the old SPDX key to a scancode
# expression.
OLD_SPDX_EXCEPTION_LICENSES_SUBS = None


def get_old_expressions_subs_table(licensing):
    global OLD_SPDX_EXCEPTION_LICENSES_SUBS
    if not OLD_SPDX_EXCEPTION_LICENSES_SUBS:
        # this is mapping an OLD SPDX id to a new SPDX expression
        EXPRESSSIONS_BY_OLD_SPDX_IDS = {k.lower(): v.lower() for k, v in {
            'eCos-2.0': 'GPL-2.0-or-later WITH eCos-exception-2.0',
            'GPL-2.0-with-autoconf-exception': 'GPL-2.0-only WITH Autoconf-exception-2.0',
            'GPL-2.0-with-bison-exception': 'GPL-2.0-only WITH Bison-exception-2.2',
            'GPL-2.0-with-classpath-exception': 'GPL-2.0-only WITH Classpath-exception-2.0',
            'GPL-2.0-with-font-exception': 'GPL-2.0-only WITH Font-exception-2.0',
            'GPL-2.0-with-GCC-exception': 'GPL-2.0-only WITH GCC-exception-2.0',
            'GPL-3.0-with-autoconf-exception': 'GPL-3.0-only WITH Autoconf-exception-3.0',
            'GPL-3.0-with-GCC-exception': 'GPL-3.0-only WITH GCC-exception-3.1',
            'wxWindows': 'LGPL-2.0-or-later WITH  WxWindows-exception-3.1',
        }.items()}

        OLD_SPDX_EXCEPTION_LICENSES_SUBS = {
            licensing.parse(k): licensing.parse(v)
            for k, v in EXPRESSSIONS_BY_OLD_SPDX_IDS.items()
        }

    return OLD_SPDX_EXCEPTION_LICENSES_SUBS


def _parse_expression(text, licensing, spdx_symbols, unknown_symbol):
    """
    Return an Expression object by parsing the `text` string using the
    `licensing` reference Licensing. Return None or raise an exception on
    errors.
    """
    if not text:
        return
    text = text.lower()
    expression = licensing.parse(text, simple=True)

    if expression is None:
        return

    # substitute old SPDX symbols with new ones if any
    old_expressions_subs = get_old_expressions_subs_table(licensing)
    updated = expression.subs(old_expressions_subs)

    # collect known symbols and build substitution table: replace known symbols
    # with a symbol wrapping a known license and unkown symbols with the
    # unknown-spdx symbol
    symbols_table = {}

    def _get_matching_symbol(_symbol):
        return spdx_symbols.get(_symbol.key.lower(), unknown_symbol)

    for symbol in licensing.license_symbols(updated, unique=True, decompose=False):
        if isinstance(symbol, LicenseWithExceptionSymbol):
            # we have two symbols:make a a new symbo, from that
            new_with = LicenseWithExceptionSymbol(
                _get_matching_symbol(symbol.license_symbol),
                _get_matching_symbol(symbol.exception_symbol)
            )

            symbols_table[symbol] = new_with
        else:
            symbols_table[symbol] = _get_matching_symbol(symbol)

    symbolized = updated.subs(symbols_table)
    return symbolized


def _reparse_invalid_expression(text, licensing, spdx_symbols, unknown_symbol):
    """
    Return an Expression object by parsing the `text` string using the
    `licensing` reference Licensing.
    Make a best attempt at parsing eventually ignoring some of the syntax.
    The `text` string is assumed to be an invalid non-parseable expression.

    Any keyword and parens will be ignored.

    Note that an expression is ALWAYS returned: if the parsing fails or some
    other error happens somehow, this function returns instead a bare
    expression made of only "unknown-spdx" symbol.
    """
    if not text:
        return

    results = licensing.simple_tokenizer(text)
    # filter tokens to keep only symbols and keywords
    tokens = [r.value for r in results if isinstance(r.value, (LicenseSymbol, Keyword))]

    # here we have a mix of keywords and symbols that does not parse correctly
    # this could be because of some imbalance or any kind of other reasons. We ignore any parens or
    # keyword and track if we have keywords or parens
    has_keywords = False
    has_symbols = False
    filtered_tokens = []
    for tok in tokens:
        if isinstance(tok, Keyword):
            has_keywords = True
            continue
        else:
            filtered_tokens.append(tok)
            has_symbols = True

    if not has_symbols:
        return unknown_symbol

    # Build and reparse a synthetic expression using a default AND as keyword.
    # This expression may not be a correct repsentation of the invalid original,
    # but it always contains an unknown symbol if this is a not a simple uboot-
    # style OR expression.
    joined_as = ' AND '
    if not has_keywords:
        # this is bare list of symbols without parens and keywords, u-boot-
        # style: we assume the OR keyword
        joined_as = ' OR '

    expression_text = joined_as.join(s.key for s in filtered_tokens)
    expression = _parse_expression(expression_text, licensing, spdx_symbols, unknown_symbol)

    # this is more than just a u-boot-style list of license keys
    if has_keywords:
        # ... so we append an arbitrary unknown-spdx symbol to witness that the
        # expression is invalid
        expression = licensing.AND(expression, unknown_symbol)

    return expression


def prepare_text(text):
    """
    Return a 2-tuple of (`prefix`, `expression_text`) built from `text` where the
    `expression_text` is prepared to be suitable for SPDX license identifier
    detection stripped from leading and trailing punctuations, normalized for
    spaces and separateed from an SPDX-License-Identifier `prefix`.
    """
    prefix, expression = split_spdx_lid(text)
    prefix = prefix.strip() if prefix is not None else prefix
    return prefix, clean_text(expression)


def clean_text(text):
    """
    Return a text suitable for SPDX license identifier detection cleaned
    from certain leading and trailing punctuations and normalized for spaces.
    """
    text = ' '.join(text.split())
    punctuation_spaces = "!\"#$%&'*,-./:;<=>?@[\\]^_`{|}~\t\r\n "
    # remove significant expression punctuations in wrong spot: closing parens
    # at head and opening parens or + at tail.
    leading_punctuation_spaces = punctuation_spaces + ")+"
    trailng_punctuation_spaces = punctuation_spaces + "("
    text = text.lstrip(leading_punctuation_spaces).rstrip(trailng_punctuation_spaces)
    # try to fix some common cases of leading and trailing missing parense
    open_parens_count = text.count('(')
    close_parens_count = text.count(')')
    if open_parens_count == 1 and not close_parens_count:
        text = text.replace('(', ' ')
    elif close_parens_count == 1 and not open_parens_count:
        text = text.replace(')', ' ')
    return ' '.join(text.split())


_split_spdx_lid = re.compile(
    '(spdx(?:\\-|\\s)+licen(?:s|c)e(?:\\-|\\s)+identifier\\s*:?\\s*)',
    re.IGNORECASE).split


def split_spdx_lid(text):
    """
    Split text if it contains an "SPDX license identifier". Return a 2-tuple if
    if there is an SPDX license identifier where the first item contains the
    "SPDX license identifier" text proper and the second item contains the
    remainder of the line (expected to be a license expression). Otherwise
    return a 2-tuple where the first item is None and the second item contains the
    orignal text.
    """
    segments = _split_spdx_lid(text)
    expression = segments[-1]
    if len(segments) > 1:
        return segments[-2], expression
    else:
        return None, text

