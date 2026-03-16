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
from commoncode.cliutils import SCAN_OPTIONS_GROUP
from plugincode.scan import ScanPlugin
from plugincode.scan import scan_impl
from commoncode.cliutils import OTHER_SCAN_GROUP


@scan_impl
class EmailScanner(ScanPlugin):
    """
    Scan a Resource for emails.
    """
    resource_attributes = dict(emails=attr.ib(default=attr.Factory(list)))

    sort_order = 8

    options = [
        PluggableCommandLineOption(('-e', '--email',),
            is_flag=True, default=False,
            help='Scan <input> for emails.',
            help_group=OTHER_SCAN_GROUP),

        PluggableCommandLineOption(('--max-email',),
            type=int, default=50,
            metavar='INT',
            show_default=True,
            required_options=['email'],
            help='Report only up to INT emails found in a file. Use 0 for no limit.',
            help_group=SCAN_OPTIONS_GROUP),
    ]

    def is_enabled(self, email, **kwargs):
        return email

    def get_scanner(self, max_email=50, test_slow_mode=False, test_error_mode=False, **kwargs):
        from scancode.api import get_emails
        return partial(
            get_emails,
            threshold=max_email,
            test_slow_mode=test_slow_mode,
            test_error_mode=test_error_mode
        )
