#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial

import attr

from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OTHER_SCAN_GROUP
from commoncode.cliutils import SCAN_OPTIONS_GROUP
from plugincode.scan import ScanPlugin
from plugincode.scan import scan_impl


@scan_impl
class UrlScanner(ScanPlugin):
    """
    Scan a Resource for URLs.
    """

    resource_attributes = dict(urls=attr.ib(default=attr.Factory(list)))

    sort_order = 10

    options = [
        PluggableCommandLineOption(('-u', '--url',),
            is_flag=True, default=False,
            help='Scan <input> for urls.',
            help_group=OTHER_SCAN_GROUP),

        PluggableCommandLineOption(('--max-url',),
            type=int, default=50,
            metavar='INT',
            required_options=['url'],
            show_default=True,
            help='Report only up to INT urls found in a file. Use 0 for no limit.',
            help_group=SCAN_OPTIONS_GROUP),
    ]

    def is_enabled(self, url, **kwargs):
        return url

    def get_scanner(self, max_url=50, **kwargs):
        from scancode.api import get_urls
        return partial(get_urls, threshold=max_url)
