#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from plugincode.output_filter import OutputFilterPlugin
from plugincode.output_filter import output_filter_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import OUTPUT_FILTER_GROUP


@output_filter_impl
class OnlyFindings(OutputFilterPlugin):
    """
    Filter files or directories without scan findings for the requested scans.
    """

    options = [
        PluggableCommandLineOption(('--only-findings',), is_flag=True,
            help='Only return files or directories with findings for the '
                 'requested scans. Files and directories without findings are '
                 'omitted (file information is not treated as findings).',
            help_group=OUTPUT_FILTER_GROUP)
    ]

    def is_enabled(self, only_findings, **kwargs):
        return only_findings

    def process_codebase(self, codebase, resource_attributes_by_plugin, **kwargs):
        """
        Set Resource.is_filtered to True for resources from the codebase that do
        not have findings e.g. if they have no scan data (cinfo) and no
        errors.
        """
        resource_attributes_with_findings = set(['scan_errors'])
        for plugin_qname, keys in resource_attributes_by_plugin.items():
            if plugin_qname == 'scan:info':
                # skip info resource_attributes
                continue
            resource_attributes_with_findings.update(keys)

        for resource in codebase.walk():
            if has_findings(resource, resource_attributes_with_findings):
                continue
            resource.is_filtered = True
            codebase.save_resource(resource)


def has_findings(resource, resource_attributes_with_findings):
    """
    Return True if this resource has findings.
    """
    attribs = (getattr(resource, key, None) for key in resource_attributes_with_findings)
    return bool(any(attribs))
