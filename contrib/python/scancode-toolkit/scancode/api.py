#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
from itertools import islice
from os.path import getsize
import logging
import os
import sys

from commoncode.filetype import get_last_modified_date
from commoncode.hash import multi_checksums
from scancode import ScancodeError
from typecode.contenttype import get_type

TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

"""
Main scanning functions.

Each scanner is a function that accepts a location and returns a sequence of
mappings as results.

Note: this API is unstable and still evolving.
"""


def get_copyrights(location, deadline=sys.maxsize, **kwargs):
    """
    Return a mapping with a single 'copyrights' key with a value that is a list
    of mappings for copyright detected in the file at `location`.
    """
    from cluecode.copyrights import detect_copyrights

    copyrights = []
    holders = []
    authors = []

    for dtype, value, start, end in detect_copyrights(location, deadline=deadline):

        if dtype == 'copyrights':
            copyrights.append(
                dict([
                    ('value', value),
                    ('start_line', start),
                    ('end_line', end)
                ])
            )
        elif dtype == 'holders':
            holders.append(
                dict([
                    ('value', value),
                    ('start_line', start),
                    ('end_line', end)
                ])
            )
        elif dtype == 'authors':
            authors.append(
                dict([
                    ('value', value),
                    ('start_line', start),
                    ('end_line', end)
                ])
            )

    results = dict([
        ('copyrights', copyrights),
        ('holders', holders),
        ('authors', authors),
    ])

    return results


def get_emails(location, threshold=50, test_slow_mode=False, test_error_mode=False, **kwargs):
    """
    Return a mapping with a single 'emails' key with a value that is a list of
    mappings for emails detected in the file at `location`.
    Return only up to `threshold` values. Return all values if `threshold` is 0.

    If test_mode is True, the scan will be slow for testing purpose and pause
    for one second.
    """
    if test_error_mode:
        raise ScancodeError('Triggered email failure')

    if test_slow_mode:
        import time
        time.sleep(1)

    from cluecode.finder import find_emails
    results = []

    found_emails = ((em, ln) for (em, ln) in find_emails(location) if em)
    if threshold:
        found_emails = islice(found_emails, threshold)

    for email, line_num in found_emails:
        result = {}
        results.append(result)
        result['email'] = email
        result['start_line'] = line_num
        result['end_line'] = line_num
    return dict(emails=results)


def get_urls(location, threshold=50, **kwargs):
    """
    Return a mapping with a single 'urls' key with a value that is a list of
    mappings for urls detected in the file at `location`.
    Return only up to `threshold` values. Return all values if `threshold` is 0.
    """
    from cluecode.finder import find_urls
    results = []

    found_urls = ((u, ln) for (u, ln) in find_urls(location) if u)
    if threshold:
        found_urls = islice(found_urls, threshold)

    for urls, line_num in found_urls:
        result = {}
        results.append(result)
        result['url'] = urls
        result['start_line'] = line_num
        result['end_line'] = line_num
    return dict(urls=results)


SPDX_LICENSE_URL = 'https://spdx.org/licenses/{}'
DEJACODE_LICENSE_URL = 'https://enterprise.dejacode.com/urn/urn:dje:license:{}'
SCANCODE_LICENSEDB_URL = 'https://scancode-licensedb.aboutcode.org/{}'


def get_licenses(location, min_score=0,
                 include_text=False, license_text_diagnostics=False,
                 license_url_template=SCANCODE_LICENSEDB_URL,
                 deadline=sys.maxsize, **kwargs):
    """
    Return a mapping or detected_licenses for licenses detected in the file at
    `location`

    This mapping contains two keys:
     - 'licenses' with a value that is list of mappings of license information.
     - 'license_expressions' with a value that is list of license expression
       strings.

    `minimum_score` is a minimum score threshold from 0 to 100. The default is 0
    means that all license matches are returned. Otherwise, matches with a score
    below `minimum_score` are returned.

    If `include_text` is True, matched text is included in the returned
    `licenses` data as well as a file-level `percentage_of_license_text` percentage to
    indicate the overall proportion of detected license text and license notice
    words in the file. This is used to determine if a file contains mostly
    licensing information.
    """
    from licensedcode import cache
    from licensedcode.spans import Span

    idx = cache.get_index()

    detected_licenses = []
    detected_expressions = []

    matches = idx.match(
        location=location, min_score=min_score, deadline=deadline, **kwargs)

    qspans = []
    match = None
    for match in matches:
        qspans.append(match.qspan)

        detected_expressions.append(match.rule.license_expression)

        detected_licenses.extend(
            _licenses_data_from_match(
                match=match,
                include_text=include_text,
                license_text_diagnostics=license_text_diagnostics,
                license_url_template=license_url_template)
        )

    percentage_of_license_text = 0
    if match:
        # we need at least one match to compute a license_coverage
        matched_tokens_length = len(Span().union(*qspans))
        query_tokens_length = match.query.tokens_length(with_unknown=True)
        percentage_of_license_text = round((matched_tokens_length / query_tokens_length) * 100, 2)

    detected_spdx_expressions = []
    return dict([
        ('licenses', detected_licenses),
        ('license_expressions', detected_expressions),
        ('spdx_license_expressions', detected_spdx_expressions),
        ('percentage_of_license_text', percentage_of_license_text),
    ])


def _licenses_data_from_match(
        match, include_text=False, license_text_diagnostics=False,
        license_url_template=SCANCODE_LICENSEDB_URL):
    """
    Return a list of "licenses" scan data built from a license match.
    Used directly only internally for testing.
    """
    from licensedcode import cache
    licenses = cache.get_licenses_db()

    matched_text = None
    if include_text:
        if license_text_diagnostics:
            matched_text = match.matched_text(whole_lines=False, highlight=True)
        else:
            matched_text = match.matched_text(whole_lines=True, highlight=False)

    SCANCODE_BASE_URL = 'https://github.com/nexB/scancode-toolkit/tree/develop/src/licensedcode/data/licenses'
    SCANCODE_LICENSE_TEXT_URL = SCANCODE_BASE_URL + '/{}.LICENSE'
    SCANCODE_LICENSE_DATA_URL = SCANCODE_BASE_URL + '/{}.yml'

    detected_licenses = []
    for license_key in match.rule.license_keys():
        lic = licenses.get(license_key)
        result = {}
        detected_licenses.append(result)
        result['key'] = lic.key
        result['score'] = match.score()
        result['name'] = lic.name
        result['short_name'] = lic.short_name
        result['category'] = lic.category
        result['is_exception'] = lic.is_exception
        result['owner'] = lic.owner
        result['homepage_url'] = lic.homepage_url
        result['text_url'] = lic.text_urls[0] if lic.text_urls else ''
        result['reference_url'] = license_url_template.format(lic.key)
        result['scancode_text_url'] = SCANCODE_LICENSE_TEXT_URL.format(lic.key)
        result['scancode_data_url'] = SCANCODE_LICENSE_DATA_URL.format(lic.key)

        spdx_key = lic.spdx_license_key
        result['spdx_license_key'] = spdx_key

        if spdx_key:
            is_license_ref = spdx_key.lower().startswith('licenseref-')
            if is_license_ref:
                spdx_url = SCANCODE_LICENSE_TEXT_URL.format(lic.key)
            else:
                spdx_key = lic.spdx_license_key.rstrip('+')
                spdx_url = SPDX_LICENSE_URL.format(spdx_key)
        else:
            spdx_url = ''
        result['spdx_url'] = spdx_url
        result['start_line'] = match.start_line
        result['end_line'] = match.end_line
        matched_rule = result['matched_rule'] = {}
        matched_rule['identifier'] = match.rule.identifier
        matched_rule['license_expression'] = match.rule.license_expression
        matched_rule['licenses'] = match.rule.license_keys()
        matched_rule['is_license_text'] = match.rule.is_license_text
        matched_rule['is_license_notice'] = match.rule.is_license_notice
        matched_rule['is_license_reference'] = match.rule.is_license_reference
        matched_rule['is_license_tag'] = match.rule.is_license_tag
        matched_rule['is_license_intro'] = match.rule.is_license_intro
        matched_rule['matcher'] = match.matcher
        matched_rule['rule_length'] = match.rule.length
        matched_rule['matched_length'] = match.len()
        matched_rule['match_coverage'] = match.coverage()
        matched_rule['rule_relevance'] = match.rule.relevance
        # FIXME: for sanity this should always be included?????
        if include_text:
            result['matched_text'] = matched_text
    return detected_licenses


SCANCODE_DEBUG_PACKAGE_API = os.environ.get('SCANCODE_DEBUG_PACKAGE_API', False)


def get_package_info(location, **kwargs):
    """
    Return a mapping of package manifest information detected in the
    file at `location`.

    Note that all exceptions are caught if there are any errors while parsing a
    package manifest.
    """
    from packagedcode.recognize import recognize_packages
    try:
        recognized_packages = recognize_packages(location)
        if recognized_packages:
            return dict(packages=[package.to_dict() for package in recognized_packages])
    except Exception as e:
        if TRACE:
            logger.error('get_package_info: {}: Exception: {}'.format(location, e))

        if SCANCODE_DEBUG_PACKAGE_API:
            raise
        else:
            # attention: we are swallowing ALL exceptions here!
            pass

    return dict(packages=[])


def get_file_info(location, **kwargs):
    """
    Return a mapping of file information collected for the file at `location`.
    """
    result = {}

    # TODO: move date and size these to the inventory collection step???
    result['date'] = get_last_modified_date(location) or None
    result['size'] = getsize(location) or 0

    sha1, md5, sha256 = multi_checksums(location, ('sha1', 'md5', 'sha256')).values()
    result['sha1'] = sha1
    result['md5'] = md5
    result['sha256'] = sha256

    collector = get_type(location)
    result['mime_type'] = collector.mimetype_file or None
    result['file_type'] = collector.filetype_file or None
    result['programming_language'] = collector.programming_language or None
    result['is_binary'] = bool(collector.is_binary)
    result['is_text'] = bool(collector.is_text)
    result['is_archive'] = bool(collector.is_archive)
    result['is_media'] = bool(collector.is_media)
    result['is_source'] = bool(collector.is_source)
    result['is_script'] = bool(collector.is_script)
    return result
