#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import attr

from plugincode.post_scan import PostScanPlugin
from plugincode.post_scan import post_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import POST_SCAN_GROUP


@post_scan_impl
class MarkSource(PostScanPlugin):
    """
    Set the "is_source" flag to true for directories that contain
    over 90% of source files as direct children.
    Has no effect unless the --info scan is requested.
    """

    resource_attributes = dict(source_count=attr.ib(default=0, type=int, repr=False))

    sort_order = 8

    options = [
        PluggableCommandLineOption(('--mark-source',),
            is_flag=True, default=False,
            required_options=['info'],
            help='Set the "is_source" to true for directories that contain '
                 'over 90% of source files as children and descendants. '
                 'Count the number of source files in a directory as a new source_file_counts attribute',
            help_group=POST_SCAN_GROUP)
    ]

    def is_enabled(self, mark_source, info, **kwargs):
        return mark_source and info

    def process_codebase(self, codebase, mark_source, **kwargs):
        """
        Set the `is_source` to True in directories if they contain over 90% of
        source code files at full depth.
        """
        for resource in codebase.walk(topdown=False):
            if resource.is_file:
                continue

            children = resource.children(codebase)
            if not children:
                continue

            src_count = sum(1 for c in children if c.is_file and c.is_source)
            src_count += sum(c.source_count for c in children if not c.is_file)
            is_source = is_source_directory(src_count, resource.files_count)

            if src_count and is_source:
                resource.is_source = is_source
                resource.source_count = src_count
                codebase.save_resource(resource)


def is_source_directory(src_count, files_count):
    """
    Return True is this resource is a source directory with at least over 90% of
    source code files at full depth.
    """
    return src_count / files_count >= 0.9
