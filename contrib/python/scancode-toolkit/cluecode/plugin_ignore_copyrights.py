#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import re


from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OUTPUT_FILTER_GROUP
from plugincode.output_filter import OutputFilterPlugin
from plugincode.output_filter import output_filter_impl


def logger_debug(*args):
    pass


TRACE = False

if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


@output_filter_impl
class IgnoreCopyrights(OutputFilterPlugin):
    """
    Filter findings that match given copyright holder or author patterns.
    Has no effect unless the --copyright scan is requested.
    """

    options = [
        PluggableCommandLineOption(('--ignore-copyright-holder',),
               multiple=True,
               metavar='<pattern>',
               help='Ignore a file (and all its findings) if a copyright holder '
               'contains a match to the <pattern> regular expression. '
               'Note that this will ignore a file even if it has other scanned '
               'data such as a license or errors.',
               help_group=OUTPUT_FILTER_GROUP),
        PluggableCommandLineOption(
            ('--ignore-author',),
            multiple=True,
            metavar='<pattern>',
            help='Ignore a file (and all its findings) if an author '
               'contains a match to the <pattern> regular expression. '
               'Note that this will ignore a file even if it has other findings '
               'such as a license or errors.',
            help_group=OUTPUT_FILTER_GROUP)
    ]

    def is_enabled(self, ignore_copyright_holder, ignore_author, **kwargs):  # NOQA
        return bool(ignore_copyright_holder or ignore_author)

    def process_codebase(self, codebase, ignore_copyright_holder, ignore_author, **kwargs):
        ignored_holders = [re.compile(r) for r in ignore_copyright_holder]
        ignored_authors = [re.compile(r) for r in ignore_author]

        for resource in codebase.walk():
            holders = set(c['value'] for c in getattr(resource, 'holders', []))
            authors = set(c['value'] for c in getattr(resource, 'authors', []))
            if TRACE:
                logger_debug('holders:', holders)
                logger_debug('authors:', authors)

            if is_ignored(ignored_holders, holders) or is_ignored(ignored_authors, authors):
                resource.is_filtered = True
                codebase.save_resource(resource)


def is_ignored(patterns, values):
    """
    Return True if any of the string in `values` matches any of the
    `patterns` list of compiled regex.
    """
    for val in values:
        # FIXME: using re.search matches anywhere. re.match matches the
        # whole string instead

        if any(p.search(val) for p in patterns):
            return True
