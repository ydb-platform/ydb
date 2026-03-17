#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from os.path import exists
from os.path import isdir

import attr
import saneyaml

from plugincode.post_scan import PostScanPlugin
from plugincode.post_scan import post_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import POST_SCAN_GROUP


@post_scan_impl
class LicensePolicy(PostScanPlugin):
    """
    Add the "license_policy" attribute to a resouce if it contains a
    detected license key that is found in the license_policy.yml file
    """

    resource_attributes = dict(license_policy=attr.ib(default=attr.Factory(dict)))

    sort_order = 9

    options = [
        PluggableCommandLineOption(('--license-policy',),
            multiple=False,
            metavar='FILE',
            help='Load a License Policy file and apply it to the scan at the '
                 'Resource level.',
            help_group=POST_SCAN_GROUP)
    ]

    def is_enabled(self, license_policy, **kwargs):
        return license_policy

    def process_codebase(self, codebase, license_policy, **kwargs):
        """
        Populate a license_policy mapping with four attributes: license_key, label,
        icon, and color_code at the File Resource level.
        """
        if not self.is_enabled(license_policy):
            return

        if has_policy_duplicates(license_policy):
            codebase.errors.append('ERROR: License Policy file contains duplicate entries.\n')
            return

        # get a list of unique license policies from the license_policy file
        policies = load_license_policy(license_policy).get('license_policies', [])

        # apply policy to Resources if they contain an offending license
        for resource in codebase.walk(topdown=True):
            if not resource.is_file:
                continue

            try:
                resource_license_keys = set([entry.get('key') for entry in resource.licenses])

            except AttributeError:
                # add license_policy regardless if there is license info or not
                resource.license_policy = {}
                codebase.save_resource(resource)
                continue

            for key in resource_license_keys:
                for policy in policies:
                    if key == policy.get('license_key'):
                        # Apply the policy to the Resource
                        resource.license_policy = policy
                        codebase.save_resource(resource)


def has_policy_duplicates(license_policy_location):
    """
    Returns True if the policy file contains duplicate entries for a specific license
    key. Returns False otherwise.
    """
    policies = load_license_policy(license_policy_location).get('license_policies', [])

    unique_policies = {}

    if policies == []:
        return False

    for policy in policies:
        license_key = policy.get('license_key')

        if license_key in unique_policies.keys():
            return True
        else:
            unique_policies[license_key] = policy

    return False


def load_license_policy(license_policy_location):
    """
    Return a license_policy dictionary loaded from a license policy file.
    """
    if not license_policy_location or not exists(license_policy_location):
        return {}
    elif isdir(license_policy_location):
        return {}
    with open(license_policy_location, 'r') as conf:
        conf_content = conf.read()
    return saneyaml.load(conf_content)
