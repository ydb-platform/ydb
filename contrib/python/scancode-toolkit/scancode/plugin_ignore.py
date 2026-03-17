#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial

from commoncode.fileset import is_included
from plugincode.pre_scan import PreScanPlugin
from plugincode.pre_scan import pre_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import PRE_SCAN_GROUP


# Tracing flags
TRACE = False


def logger_debug(*args):
    pass


if TRACE:
    import logging
    import sys

    logger = logging.getLogger(__name__)
    # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


@pre_scan_impl
class ProcessIgnore(PreScanPlugin):
    """
    Include or ignore files matching patterns.
    """

    options = [
        PluggableCommandLineOption(('--ignore',),
           multiple=True,
           metavar='<pattern>',
           help='Ignore files matching <pattern>.',
           sort_order=10,
           help_group=PRE_SCAN_GROUP),
        PluggableCommandLineOption(('--include',),
           multiple=True,
           metavar='<pattern>',
           help='Include files matching <pattern>.',
           sort_order=11,
           help_group=PRE_SCAN_GROUP)
    ]

    def is_enabled(self, ignore, include, **kwargs):
        return ignore or include

    def process_codebase(self, codebase, ignore=(), include=(), **kwargs):
        """
        Keep only included and non-ignored Resources in the codebase.
        """

        if not (ignore or include):
            return

        excludes = {
            pattern: 'User ignore: Supplied by --ignore' for pattern in ignore
        }

        includes = {
            pattern: 'User include: Supplied by --include' for pattern in include
        }

        included = partial(is_included, includes=includes, excludes=excludes)

        rids_to_remove = set()
        rids_to_remove_add = rids_to_remove.add
        rids_to_remove_discard = rids_to_remove.discard

        # First, walk the codebase from the top-down and collect the rids of
        # Resources that can be removed.
        for resource in codebase.walk(topdown=True):
            if resource.is_root:
                continue
            resource_rid = resource.rid

            if not included(resource.path):
                for child in resource.children(codebase):
                    rids_to_remove_add(child.rid)
                rids_to_remove_add(resource_rid)
            else:
                # we may have been selected for removal based on a parent dir
                # but may be explicitly included. Honor that
                rids_to_remove_discard(resource_rid)
        if TRACE:
            logger_debug('process_codebase: rids_to_remove')
            logger_debug(rids_to_remove)
            for rid in sorted(rids_to_remove):
                logger_debug(codebase.get_resource(rid))

        remove_resource = codebase.remove_resource

        # Then, walk bottom-up and remove the non-included Resources from the
        # Codebase if the Resource's rid is in our list of rid's to remove.
        for resource in codebase.walk(topdown=False):
            resource_rid = resource.rid
            if resource.is_root:
                continue
            if resource.is_dir:  # removing dirs will also remove its files
                continue
            if resource_rid in rids_to_remove:
                rids_to_remove_discard(resource_rid)
                remove_resource(resource)
