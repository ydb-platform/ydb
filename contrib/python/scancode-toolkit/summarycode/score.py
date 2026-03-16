#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from itertools import chain

import attr
from license_expression import Licensing

from commoncode.datautils import Mapping
from licensedcode.cache import get_licenses_db
from licensedcode import models
from plugincode.post_scan import PostScanPlugin
from plugincode.post_scan import post_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import POST_SCAN_GROUP
from summarycode import facet


# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))

"""
A plugin to compute a licensing clarity score as designed in ClearlyDefined
"""


# minimum score to consider a license detection as good.

# MIN_GOOD_LICENSE_SCORE = 80

@attr.s(slots=True)
class LicenseFilter(object):
    min_score = attr.ib(default=0)
    min_coverage = attr.ib(default=0)
    min_relevance = attr.ib(default=0)


FILTERS = dict(
    is_license_text=LicenseFilter(min_score=70, min_coverage=80),
    is_license_notice=LicenseFilter(min_score=80, min_coverage=80),
    is_license_tag=LicenseFilter(min_coverage=100),
    is_license_reference=LicenseFilter(min_score=50, min_coverage=100),
    is_license_intro=LicenseFilter(min_score=100, min_coverage=100),
)


def is_good_license(detected_license):
    """
    Return True if a `detected license` mapping is consider to a high quality
    conclusive match.
    """
    score = detected_license['score']
    rule = detected_license['matched_rule']
    coverage = rule.get('match_coverage') or 0
    relevance = rule.get('rule_relevance') or 0
    match_types = dict([
        ('is_license_text', rule['is_license_text']),
        ('is_license_notice', rule['is_license_notice']),
        ('is_license_reference', rule['is_license_reference']),
        ('is_license_tag', rule['is_license_tag']),
        ('is_license_intro', rule['is_license_intro']),
    ])
    matched = False
    for match_type, mval in match_types.items():
        if mval:
            matched = True
            break
    if not matched:
        return False

    thresholds = FILTERS[match_type]

    if not coverage or not relevance:
        if score >= thresholds.min_score:
            return True
    else:
        if (score >= thresholds.min_score
        and coverage >= thresholds.min_coverage
        and relevance >= thresholds.min_relevance):
            return True

    return False


@post_scan_impl
class LicenseClarityScore(PostScanPlugin):
    """
    Compute a License clarity score at the codebase level.
    """
    codebase_attributes = dict(license_clarity_score=Mapping(
        help='Computed license clarity score as mapping containing the score '
             'proper and each scoring elements.'))

    sort_order = 110

    options = [
        PluggableCommandLineOption(('--license-clarity-score',),
            is_flag=True,
            default=False,
            help='Compute a summary license clarity score at the codebase level.',
            help_group=POST_SCAN_GROUP,
            required_options=[
                'classify',
            ],
        )
    ]

    def is_enabled(self, license_clarity_score, **kwargs):
        return license_clarity_score

    def process_codebase(self, codebase, license_clarity_score, **kwargs):
        if TRACE:
            logger_debug('LicenseClarityScore:process_codebase')
        scoring_elements = compute_license_score(codebase)
        codebase.attributes.license_clarity_score.update(scoring_elements)


def compute_license_score(codebase):
    """
    Return a mapping of scoring elements and a license clarity score computed at
    the codebase level.
    """

    score = 0
    scoring_elements = dict(score=score)

    for element in SCORING_ELEMENTS:
        element_score = element.scorer(codebase)
        if element.is_binary:
            scoring_elements[element.name] = bool(element_score)
            element_score = 1 if element_score else 0
        else:
            scoring_elements[element.name] = round(element_score, 2) or 0

        score += int(element_score * element.weight)
        if TRACE:
            logger_debug(
                'compute_license_score: element:', element, 'element_score: ',
                element_score, ' new score:', score)

    scoring_elements['score'] = score or 0
    return scoring_elements


def get_declared_license_keys(codebase):
    """
    Return a list of declared license keys found in packages and key files.
    """
    return (
        get_declared_license_keys_in_key_files(codebase) +
        get_declared_license_keys_in_packages(codebase)
    )


def get_declared_license_keys_in_packages(codebase):
    """
    Return a list of declared license keys found in packages.

    A package manifest (such as Maven POM file or an npm package.json file)
    contains structured declared license information. This is further normalized
    as a license_expression. We extract the list of licenses from the normalized
    license expressions.
    """
    packages = chain.from_iterable(
        getattr(res, 'packages', []) or []
        for res in codebase.walk(topdown=True))

    licensing = Licensing()
    detected_good_licenses = []
    for package in packages:
        expression = package.get('license_expression')
        if expression:
            exp = licensing.parse(
                expression, validate=False, strict=False, simple=True)
            keys = licensing.license_keys(exp, unique=True)
            detected_good_licenses.extend(keys)
    return detected_good_licenses


def get_declared_license_keys_in_key_files(codebase):
    """
    Return a list of "declared" license keys from the expressions as detected in
    key files.

    A project has specific key file(s) at the top level of its code hierarchy
    such as LICENSE, NOTICE or similar (and/or a package manifest) containing
    structured license information such as an SPDX license expression or SPDX
    license identifier: when such a file contains "clearly defined" declared
    license information, we return this.

    Note: this ignores facets.
    """
    declared = []
    for resource in codebase.walk(topdown=True):
        if not resource.is_key_file:
            continue

        for detected_license in getattr(resource, 'licenses', []) or []:
            if not is_good_license(detected_license):
                declared.append('unknown')
            else:
                declared.append(detected_license['key'])
    return declared


def is_core_facet(resource, core_facet=facet.FACET_CORE):
    """
    Return True if the resource is in the core facet.
    If we do not have facets, everything is considered as being core by default.
    """
    has_facets = hasattr(resource, 'facets')
    if not has_facets:
        return True
    # facets is a list
    return not resource.facets or core_facet in resource.facets


def has_good_licenses(resource):
    """
    Return True if a Resource licenses are all detected as a "good license"
    detection-wise.
    """
    licenses = getattr(resource, 'licenses', []) or []

    if not licenses:
        return False

    for detected_license in licenses:
        # the license score must be above some threshold
        if not is_good_license(detected_license):
            return False
        # and not an "unknown" license
        if is_unknown_license(detected_license['key']):
            return False
    return True


def is_unknown_license(lic_key):
    """
    Return True if a license key is for some lesser known or unknown license.
    """
    return lic_key.startswith(('unknown', 'other-',)) or 'unknown' in lic_key


def has_unkown_licenses(resource):
    """
    Return True if some Resource licenses are unknown.
    """
    return not any(is_unknown_license(lic['key'])
                   for lic in getattr(resource, 'licenses', []) or [])


_spdx_keys = None


def get_spdx_keys():
    """
    Return a set of ScanCode license keys for licenses that are listed in SPDX.
    """
    global _spdx_keys
    if not _spdx_keys:
        _spdx_keys = frozenset(models.get_all_spdx_keys(get_licenses_db()))
    return _spdx_keys


def is_using_only_spdx_licenses(codebase):
    """
    Return True if all file-level detected licenses are SPDX licenses.
    """
    licenses = chain.from_iterable(
        res.licenses for res in codebase.walk() if res.is_file)
    keys = set(l['key'] for l in licenses)
    spdx_keys = get_spdx_keys()
    return keys and spdx_keys and all(k in spdx_keys for k in keys)


def has_consistent_key_and_file_level_licenses(codebase):
    """
    Return True if the file-level licenses are consistent with top level key
    files licenses.
    """
    key_files_licenses, other_files_licenses = get_unique_licenses(codebase)

    if (key_files_licenses
    and key_files_licenses == other_files_licenses
    and not any(is_unknown_license(l) for l in key_files_licenses)):
        return True
    else:
        return False


def get_unique_licenses(codebase, good_only=True):
    """
    Return a tuple of two sets of license keys found in the codebase:
    - the set license found in key files
    - the set license found in non-key files

    This is only for files in the core facet.
    """
    key_license_keys = set()
    other_license_keys = set()

    for resource in codebase.walk():
        # FIXME: consider only text, source-like files for now
        if not resource.is_file:
            continue
        if not (resource.is_key_file or is_core_facet(resource)):
            # we only cover either core code/core facet or top level, key files
            continue

        if resource.is_key_file:
            license_keys = key_license_keys
        else:
            license_keys = other_license_keys

        for detected_license in getattr(resource, 'licenses', []) or []:
            if good_only and not is_good_license(detected_license):
                license_keys.add('unknown')
            else:
                license_keys.add(detected_license['key'])

    return key_license_keys, other_license_keys


def get_detected_license_keys_with_full_text(codebase, key_files_only=False, good_only=True):
    """
    Return a set of license keys for which at least one detection includes the
    full license text.

    This is for any files in the core facet or not.
    """
    license_keys = set()

    for resource in codebase.walk():
        # FIXME: consider only text, source-like files for now
        if not resource.is_file:
            continue

        if key_files_only and not resource.is_key_file:
            continue

        for detected_license in getattr(resource, 'licenses', []) or []:
            if good_only and not is_good_license(detected_license):
                continue
            if detected_license['matched_rule']['is_license_text']:
                license_keys.add(detected_license['key'])

    return license_keys


def has_full_text_in_key_files_for_all_licenses(codebase):
    """
    Return True if the full text of all licenses is preset in the codebase key,
    top level files.
    """
    return _has_full_text(codebase, key_files_only=True)


def has_full_text_for_all_licenses(codebase):
    """
    Return True if the full text of all licenses is preset in the codebase.
    """
    return _has_full_text(codebase, key_files_only=False)


def _has_full_text(codebase, key_files_only=False):
    """
    Return True if the full text of all licenses is preset in the codebase.
    Consider only key files if key_files_only is True.
    """

    # consider all licenses, not only good ones
    key_files_licenses, other_files_licenses = get_unique_licenses(
        codebase, good_only=False)

    if TRACE:
        logger_debug(
            '_has_full_text: key_files_licenses:', key_files_licenses,
            'other_files_licenses:', other_files_licenses)

    all_keys = key_files_licenses | other_files_licenses
    if not all_keys:
        return False

    if TRACE:
        logger_debug(
            '_has_full_text: all_keys:', all_keys)

    keys_with_license_text = get_detected_license_keys_with_full_text(
        codebase, key_files_only, good_only=False)

    if TRACE:
        logger_debug(
            '_has_full_text: keys_with_license_text:', keys_with_license_text)
        logger_debug(
            '_has_full_text: all_keys == keys_with_license_text:',
            all_keys == keys_with_license_text)

    return all_keys == keys_with_license_text


def get_file_level_license_and_copyright_coverage(codebase):
    """
    Return a float between 0 and 1 that represent the proportions of files that
    have a license and a copyright vs. all files.
    """
    scoring_element = 0
    covered_files, files_count = get_other_licenses_and_copyrights_counts(codebase)

    if TRACE:
        logger_debug('compute_license_score:covered_files:',
                     covered_files, 'files_count:', files_count)

    if files_count:
        # avoid floats for zero
        scoring_element = (covered_files / files_count) or 0

        if TRACE:
            logger_debug('compute_license_score:scoring_element:', scoring_element)

    return scoring_element


def get_other_licenses_and_copyrights_counts(codebase):
    """
    Return a tuple of (count of files with a license/copyright, total count of
    files).

    Do files that can contain licensing and copyright information reliably carry
    such information? This is based on a percentage of files in the core facet
    of the project that have both:

    - A license text, notice or an SPDX-License-Identifier and,
    - A copyright statement in standard (e.g. recognized) format.

    Here "reliably" means that these are reliably detected by tool(s) with a
    high level of confidence This is a progressive element that is computed
    based on:

    - LICCOP:  the number of files with a license notice and copyright statement
    - TOT: the total number of files

    """
    total_files_count = 0
    files_with_good_license_and_copyright_count = 0
    files_with_a_license_count = 0
    files_with_a_good_license_count = 0
    files_with_a_copyright_count = 0

    for resource in codebase.walk():
        # consider non-key files
        if resource.is_key_file or not resource.is_file:
            continue

        # ... in the core facet
        if not is_core_facet(resource):
            continue

        total_files_count += 1

        licenses = getattr(resource, 'licenses', []) or []
        # ... with a license
        if licenses:
            files_with_a_license_count += 1

        is_public_domain = [l['key'] for l in licenses] == 'public-domain'

        copyrights = getattr(resource, 'copyrights', []) or []

        # ... with a copyright, unless public-domain
        if copyrights or (not copyrights and is_public_domain):
            files_with_a_copyright_count += 1

        # ... where the license is a "good one"
        if has_good_licenses(resource):
            files_with_a_good_license_count += 1
            if copyrights:
                files_with_good_license_and_copyright_count += 1

    return files_with_good_license_and_copyright_count, total_files_count


@attr.s
class ScoringElement(object):
    is_binary = attr.ib()
    name = attr.ib()
    scorer = attr.ib()
    weight = attr.ib()


declared = ScoringElement(
    is_binary=True,
    name='declared',
    scorer=get_declared_license_keys,
    weight=30)


discovered = ScoringElement(
    is_binary=False,
    name='discovered',
    scorer=get_file_level_license_and_copyright_coverage,
    weight=25)


consistency = ScoringElement(
    is_binary=True,
    name='consistency',
    scorer=has_consistent_key_and_file_level_licenses,
    weight=15)


spdx_license = ScoringElement(
    is_binary=True,
    name='spdx',
    scorer=is_using_only_spdx_licenses,
    weight=15)


full_text = ScoringElement(
    is_binary=True,
    name='license_texts',
    scorer=has_full_text_for_all_licenses,
    weight=15)


# not used for now
unknown = ScoringElement(
    is_binary=True,
    name='unknown',
    scorer=has_unkown_licenses,
    weight=15)


# not used for now
full_text_in_key_files = ScoringElement(
    is_binary=True,
    name='license_text_in_key_files',
    scorer=has_full_text_in_key_files_for_all_licenses,
    weight=15)


SCORING_ELEMENTS = [
    declared,
    discovered,
    consistency,
    spdx_license,
    full_text
]
