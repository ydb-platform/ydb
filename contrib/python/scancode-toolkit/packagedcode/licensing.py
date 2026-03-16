#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging

from license_expression import Licensing

from licensedcode.spans import Span

"""
Detect and normalize licenses as found in package manifests data.
"""

TRACE = False


def logger_debug(*args):
    pass


logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a)
                                     for a in args))


def matches_have_unknown(matches, licensing):
    """
    Return True if any of the LicenseMatch in `matches` has an unknown license.
    """
    for match in matches:
        exp = match.rule.license_expression_object
        if any(key in ('unknown', 'unknown-spdx') for key in licensing.license_keys(exp)):
            return True


def get_normalized_expression(query_string, try_as_expression=True, approximate=True):
    """
    Given a text `query_string` return a single detected license expression.
    `query_string` is typically the value of a license field as found in package
    manifests.

    If `try_as_expression` is True try first to parse this as a license
    expression.

    If `approximate` is True, also include approximate license detection as
    part of the matching procedure.

    Return None if the `query_string` is empty. Return "unknown" as a license
    expression if there is a `query_string` but nothing was detected.
    """
    if not query_string or not query_string.strip():
        return

    if TRACE:
        logger_debug(f'get_normalized_expression: query_string: "{query_string}"')

    from licensedcode.cache import get_index
    idx = get_index()
    licensing = Licensing()

    # we match twice in a cascade: as an expression, then as plain text if we
    # did not succeed.
    matches = None
    if try_as_expression:
        try:
            matched_as_expression = True
            matches = idx.match(
                query_string=query_string,
                as_expression=True,
            )
            if matches_have_unknown(matches, licensing):
                # rematch also if we have unknowns
                matched_as_expression = False
                matches = idx.match(
                    query_string=query_string,
                    as_expression=False,
                    approximate=approximate,
                )

        except Exception:
            matched_as_expression = False
            matches = idx.match(
                query_string=query_string,
                as_expression=False,
                approximate=approximate,
            )
    else:
        matched_as_expression = False
        matches = idx.match(
            query_string=query_string,
            as_expression=False,
            approximate=approximate,
        )

    if not matches:
        # we have a query_string text but there was no match: return an unknown
        # key
        return 'unknown'

    if TRACE:
        logger_debug('get_normalized_expression: matches:', matches)

    # join the possible multiple detected license expression with an AND
    expression_objects = [m.rule.license_expression_object for m in matches]
    if len(expression_objects) == 1:
        combined_expression_object = expression_objects[0]
    else:
        combined_expression_object = licensing.AND(*expression_objects)

    if matched_as_expression:
        # then just return the expression(s)
        return str(combined_expression_object)

    # Otherwise, verify that we consumed 100% of the query string e.g. that we
    # have no unknown leftover.

    # 1. have all matches 100% coverage?
    all_matches_have_full_coverage = all(m.coverage() == 100 for m in matches)

    # TODO: have all matches a high enough score?

    # 2. are all declared license tokens consumed?
    query = matches[0].query
    # the query object should be the same for all matches. Is this always true??
    for mt in matches:
        if mt.query != query:
            # FIXME: the expception may be swallowed in callers!!!
            raise Exception(
                'Inconsistent package.declared_license: text with multiple "queries".'
                'Please report this issue to the scancode-toolkit team.\n'
                f'{query_string}')

    query_len = len(query.tokens)
    matched_qspans = [m.qspan for m in matches]
    matched_qpositions = Span.union(*matched_qspans)
    len_all_matches = len(matched_qpositions)
    declared_license_is_fully_matched = query_len == len_all_matches

    if not all_matches_have_full_coverage or not declared_license_is_fully_matched:
        # We inject an 'unknown' symbol in the expression
        unknown = licensing.parse('unknown', simple=True)
        combined_expression_object = licensing.AND(combined_expression_object, unknown)

    return str(combined_expression_object)
