#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#


import attr

from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import SCAN_GROUP
from plugincode.scan import ScanPlugin
from plugincode.scan import scan_impl


@scan_impl
class CopyrightScanner(ScanPlugin):
    """
    Scan a Resource for copyrights.
    """

    resource_attributes = dict([
        ('copyrights',attr.ib(default=attr.Factory(list))),
        ('holders',attr.ib(default=attr.Factory(list))),
        ('authors',attr.ib(default=attr.Factory(list))),
    ])

    sort_order = 4

    options = [
        PluggableCommandLineOption(('-c', '--copyright',),
            is_flag=True, default=False,
            help='Scan <input> for copyrights.',
            help_group=SCAN_GROUP,
            sort_order=50),
    ]

    def is_enabled(self, copyright, **kwargs):  # NOQA
        return copyright

    def get_scanner(self, **kwargs):
        from scancode.api import get_copyrights
        return get_copyrights
