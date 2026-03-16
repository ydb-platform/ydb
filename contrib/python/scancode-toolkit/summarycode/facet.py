#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import defaultdict

import attr
import click

from commoncode.fileset import get_matches as get_fileset_matches
from plugincode.pre_scan import PreScanPlugin
from plugincode.pre_scan import pre_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import PRE_SCAN_GROUP

# Tracing flag
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
Assign a facet to a file.

A facet is defined by zero or more glob/fnmatch expressions. Multiple facets can
be assigned to a file. The facets definition is a list of (facet, pattern) and a
file is assigned all thye facets that have a pattern defintion that match their
path.

Once all files have been assigned a facet, files without a facet are assigned to
the core facet.

The known facets are:

    - core - core files of a package. Used as default if no other facet apply.
    - data - data files of a package (such as CSV, etc).
    - dev - files used at development time (e.g. build scripts, dev tools, etc)
    - docs - Documentation files.
    - examples - Code example files.
    - tests - Test files and tools.
    - thirdparty - Embedded code from a third party (aka. vendored or bundled)

See also https://github.com/clearlydefined/clearlydefined/blob/8f58a9a216cf7c129fe2cf6abe1cc6f960535e0b/docs/clearly.md#facets
"""

FACET_CORE = 'core'
FACET_DEV = 'dev'
FACET_TESTS = 'tests'
FACET_DOCS = 'docs'
FACET_DATA = 'data'
FACET_EXAMPLES = 'examples'

FACETS = (
    FACET_CORE,
    FACET_DEV,
    FACET_TESTS,
    FACET_DOCS,
    FACET_DATA,
    FACET_EXAMPLES,
)


def validate_facets(ctx, param, value):
    """
    Return the facets if valid or raise a UsageError otherwise.
    Validate facets values against the list of known facets.
    """
    if not value:
        return

    _facet_patterns, invalid_facet_definitions = build_facets(value)
    if invalid_facet_definitions:
        known_msg = ', '.join(FACETS)
        uf = '\n'.join(sorted('  ' + x for x in invalid_facet_definitions))
        msg = ('Invalid --facet option(s):\n'
               '{uf}\n'
               'Valid <facet> values are: {known_msg}.\n'.format(**locals()))
        raise click.UsageError(msg)
    return value


@pre_scan_impl
class AddFacet(PreScanPlugin):
    """
    Assign one or more "facet" to each file (and NOT to directories). Facets are
    a way to qualify that some part of the scanned code may be core code vs.
    test vs. data, etc.
    """

    resource_attributes = dict(facets=attr.ib(default=attr.Factory(list), repr=False))

    sort_order = 20

    options = [
        PluggableCommandLineOption(('--facet',),
           multiple=True,
           metavar='<facet>=<pattern>',
           callback=validate_facets,
           help='Add the <facet> to files with a path matching <pattern>.',
           help_group=PRE_SCAN_GROUP,
           sort_order=80,
        )
    ]

    def is_enabled(self, facet, **kwargs):
        if TRACE:
            logger_debug('is_enabled: facet:', facet)

        return bool(facet)

    def process_codebase(self, codebase, facet=(), **kwargs):
        """
        Add facets to file resources using the `facet` definition of facets.
        Each entry in the `facet` sequence is a string as in <facet>:<pattern>
        """

        if not facet:
            return

        facet_definitions, _invalid_facet_definitions = build_facets(facet)

        if TRACE:
            logger_debug('facet_definitions:', facet_definitions)

        # Walk the codebase and set the facets for each file (and only files)
        for resource in codebase.walk(topdown=True):
            if not resource.is_file:
                continue
            facets = compute_path_facets(resource.path, facet_definitions)
            if facets:
                resource.facets = facets
            else:
                resource.facets = [FACET_CORE]
            resource.save(codebase)


def compute_path_facets(path, facet_definitions):
    """
    Return a sorted list of unique facet strings for `path` using the
    `facet_definitions` mapping of {pattern: [facet, facet]}.
    """

    if not path or not path.strip() or not facet_definitions:
        return []

    facets = set()
    for matches in get_fileset_matches(path, facet_definitions, all_matches=True):
        facets.update(matches)
    return sorted(facets)


def build_facets(facets, known_facet_names=FACETS):
    """
    Return:
    - a mapping for facet patterns  to a list of unique facet names as 
      {pattern: [facet, facet, ...]}
    - a sorted list of error messages for invalid or unknown facet definitions
      found in `facets`.
      The `known` facets set of known facets is used for validation.
    """
    invalid_facet_definitions = set()
    facet_patterns = defaultdict(list)
    for facet_def in facets:
        facet, _, pattern = facet_def.partition('=')
        facet = facet.strip().lower()
        pattern = pattern.strip()

        if not pattern:
            invalid_facet_definitions.add(
                'missing <pattern> in "{facet_def}".'.format(**locals()))
            continue

        if not facet:
            invalid_facet_definitions.add(
                'missing <facet> in "{facet_def}".'.format(**locals()))
            continue

        if facet not in known_facet_names:
            invalid_facet_definitions.add(
                'unknown <facet> in "{facet_def}".'.format(**locals()))
            continue

        facets = facet_patterns[pattern]

        if facet not in facets:
            facet_patterns[pattern].append(facet)

    return facet_patterns, sorted(invalid_facet_definitions)
