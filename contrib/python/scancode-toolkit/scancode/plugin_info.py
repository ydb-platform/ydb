#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#


import attr

from plugincode.scan import ScanPlugin
from plugincode.scan import scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OTHER_SCAN_GROUP


@scan_impl
class InfoScanner(ScanPlugin):
    """
    Scan a file Resource for miscellaneous information such as mime/filetype and
    basic checksums.
    """
    resource_attributes = dict([
        ('date', attr.ib(default=None, repr=False)),
        ('sha1', attr.ib(default=None, repr=False)),
        ('md5', attr.ib(default=None, repr=False)),
        ('sha256', attr.ib(default=None, repr=False)),
        ('mime_type', attr.ib(default=None, repr=False)),
        ('file_type', attr.ib(default=None, repr=False)),
        ('programming_language', attr.ib(default=None, repr=False)),
        ('is_binary', attr.ib(default=False, type=bool, repr=False)),
        ('is_text', attr.ib(default=False, type=bool, repr=False)),
        ('is_archive', attr.ib(default=False, type=bool, repr=False)),
        ('is_media', attr.ib(default=False, type=bool, repr=False)),
        ('is_source', attr.ib(default=False, type=bool, repr=False)),
        ('is_script', attr.ib(default=False, type=bool, repr=False)),
    ])

    sort_order = 0

    options = [
        PluggableCommandLineOption(('-i', '--info'),
            is_flag=True, default=False,
            help='Scan <input> for file information (size, checksums, etc).',
            help_group=OTHER_SCAN_GROUP, sort_order=10
            )
    ]

    def is_enabled(self, info, **kwargs):
        return info

    def get_scanner(self, **kwargs):
        from scancode.api import get_file_info
        return get_file_info
