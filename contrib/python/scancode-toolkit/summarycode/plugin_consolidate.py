#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import Counter

import attr

from cluecode.copyrights import CopyrightDetector
from commoncode.text import python_safe_name
from license_expression import Licensing
from packagedcode import get_package_instance
from packagedcode.build import BaseBuildManifestPackage
from packagedcode.utils import combine_expressions
from plugincode.post_scan import PostScanPlugin
from plugincode.post_scan import post_scan_impl
from commoncode.cliutils import PluggableCommandLineOption
from commoncode.cliutils import POST_SCAN_GROUP
from summarycode import copyright_summary


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
        return logger.debug(
            ' '.join(isinstance(a, str) and a or repr(a) for a in args))


@attr.s
class Consolidation(object):
    """
    A grouping of files that share the same origin. Other clues found in the
    codebase are stored in `other_license_expression` and `other_holders`
    """
    identifier = attr.ib(default=None)
    consolidated_license_expression = attr.ib(default=None)
    consolidated_holders = attr.ib(default=attr.Factory(list))
    consolidated_copyright = attr.ib(default=None)
    core_license_expression = attr.ib(default=None)
    core_holders = attr.ib(default=attr.Factory(list))
    other_license_expression = attr.ib(default=None)
    other_holders = attr.ib(default=attr.Factory(list))
    files_count = attr.ib(default=None)
    resources = attr.ib(default=attr.Factory(list))

    def to_dict(self, **kwargs):
        def dict_fields(attr, value):
            if attr.name in ('resources', ):
                return False
            return True
        license_expressions_to_combine = []
        if self.core_license_expression:
            license_expressions_to_combine.append(self.core_license_expression)
        if self.other_license_expression:
            license_expressions_to_combine.append(self.other_license_expression)
        if license_expressions_to_combine:
            combined_license_expression = combine_expressions(license_expressions_to_combine)
            if combined_license_expression:
                self.consolidated_license_expression = str(Licensing().parse(combined_license_expression).simplify())
        self.core_holders = [h.original for h in self.core_holders]
        self.other_holders = [h.original for h in self.other_holders]
        self.consolidated_holders = sorted(set(self.core_holders + self.other_holders))
        # TODO: Verify and test that we are generating detectable copyrights
        self.consolidated_copyright = 'Copyright (c) {}'.format(', '.join(self.consolidated_holders))
        return attr.asdict(self, filter=dict_fields, dict_factory=dict)


@attr.s
class ConsolidatedComponent(object):
    # TODO: have an attribute for key files (one that strongly determines origin)
    type=attr.ib()
    consolidation = attr.ib()

    def to_dict(self, **kwargs):
        c = dict(type=self.type)
        c.update(self.consolidation.to_dict())
        return c


@attr.s
class ConsolidatedPackage(object):
    package = attr.ib()
    consolidation = attr.ib()

    def to_dict(self, **kwargs):
        package = self.package.to_dict()
        package.update(self.consolidation.to_dict())
        return package


@post_scan_impl
class Consolidator(PostScanPlugin):
    """
    A ScanCode post-scan plugin to return consolidated components and consolidated
    packages for different types of codebase summarization.

    A consolidated component is a group of Resources that have the same origin.
    Currently, a ConsolidatedComponent is created for each detected copyright holder
    in a codebase and contains resources that have that particular copyright holder.

    A consolidated package is a detected package in the scanned codebase that has
    been enhanced with data about other licenses and holders found within it.

    If a Resource is part of a consolidated component or consolidated package, then
    the identifier of the consolidated component or consolidated package it is part
    of is in the Resource's ``consolidated_to`` field.
    """
    codebase_attributes = dict(
        consolidated_components=attr.ib(default=attr.Factory(list)),
        consolidated_packages=attr.ib(default=attr.Factory(list))
    )

    resource_attributes = dict(
        consolidated_to=attr.ib(default=attr.Factory(list))
    )

    sort_order = 8

    options = [
        PluggableCommandLineOption(('--consolidate',),
            is_flag=True, default=False,
            help='Group resources by Packages or license and copyright holder and '
                 'return those groupings as a list of consolidated packages and '
                 'a list of consolidated components. '
                 'This requires the scan to have/be run with the copyright, license, and package options active',
            help_group=POST_SCAN_GROUP
        )
    ]

    def is_enabled(self, consolidate, **kwargs):
        return consolidate

    def process_codebase(self, codebase, **kwargs):
        # Collect ConsolidatedPackages and ConsolidatedComponents
        # TODO: Have a "catch-all" Component for the things that we haven't grouped
        consolidations = []
        root = codebase.root
        if hasattr(root, 'packages') and hasattr(root, 'copyrights') and hasattr(root, 'licenses'):
            consolidations.extend(get_consolidated_packages(codebase))
        if hasattr(root, 'copyrights') and hasattr(root, 'licenses'):
            consolidations.extend(get_holders_consolidated_components(codebase))

        if not consolidations:
            return

        # Sort consolidations by holders for consistent ordering before enumeration
        consolidations = sorted(consolidations, key=lambda c: '_'.join(h.key for h in c.consolidation.core_holders))

        # Add ConsolidatedPackages and ConsolidatedComponents to top-level codebase attributes
        codebase.attributes.consolidated_packages = consolidated_packages = []
        codebase.attributes.consolidated_components = consolidated_components = []
        identifier_counts = Counter()
        for index, c in enumerate(consolidations, start=1):
            # Skip consolidation if it does not have any Files
            if c.consolidation.files_count == 0:
                continue
            if isinstance(c, ConsolidatedPackage):
                # We use the purl as the identifier for ConsolidatedPackages
                purl = c.package.purl
                identifier_counts[purl] += 1
                identifier = python_safe_name('{}_{}'.format(purl, identifier_counts[purl]))
                c.consolidation.identifier = identifier
                for resource in c.consolidation.resources:
                    resource.consolidated_to.append(identifier)
                    resource.save(codebase)
                consolidated_packages.append(c.to_dict())
            elif isinstance(c, ConsolidatedComponent):
                consolidation_identifier = c.consolidation.identifier
                if consolidation_identifier:
                    # Use existing identifier
                    identifier_counts[consolidation_identifier] += 1
                    identifier = python_safe_name('{}_{}'.format(consolidation_identifier, identifier_counts[consolidation_identifier]))
                else:
                    # Create identifier if we don't have one
                    # TODO: Consider adding license expression to be part of name
                    holders = '_'.join(h.key for h in c.consolidation.core_holders)
                    other_holders = '_'.join(h.key for h in c.consolidation.other_holders)
                    holders = holders or other_holders
                    # We do not want the name to be too long
                    holders = holders[:65]
                    if holders:
                        # We use holders as the identifier for ConsolidatedComponents
                        identifier_counts[holders] += 1
                        identifier = python_safe_name('{}_{}'.format(holders, identifier_counts[holders]))
                    else:
                        # If we can't use holders, we use the ConsolidatedComponent's position
                        # in the list of Consolidations
                        identifier = index
                c.consolidation.identifier = identifier
                for resource in c.consolidation.resources:
                    resource.consolidated_to.append(identifier)
                    resource.save(codebase)
                consolidated_components.append(c.to_dict())

        # Dedupe and sort names in consolidated_to
        for resource in codebase.walk(topdown=True):
            resource.consolidated_to = sorted(set(resource.consolidated_to))
            resource.save(codebase)


def get_consolidated_packages(codebase):
    """
    Yield a ConsolidatedPackage for each detected package in the codebase
    """
    for resource in codebase.walk(topdown=False):
        for package_data in resource.packages:
            package = get_package_instance(package_data)
            package_root = package.get_package_root(resource, codebase)
            package_root.extra_data['package_root'] = True
            package_root.save(codebase)
            is_build_file = isinstance(package, BaseBuildManifestPackage)
            package_resources = list(package.get_package_resources(package_root, codebase))
            package_license_expression = package.license_expression
            package_copyright = package.copyright

            package_holders = []
            if package_copyright:
                numbered_lines = [(0, package_copyright)]
                for _, holder, _, _ in CopyrightDetector().detect(numbered_lines,
                        copyrights=False, holders=True, authors=False, include_years=False):
                    package_holders.append(holder)
            package_holders = process_holders(package_holders)

            discovered_license_expressions = []
            discovered_holders = []
            for package_resource in package_resources:
                if not is_build_file:
                    # If a resource is part of a package Component, then it cannot be part of any other type of Component
                    package_resource.extra_data['in_package_component'] = True
                    package_resource.save(codebase)
                if package_resource.license_expressions:
                    package_resource_license_expression = combine_expressions(package_resource.license_expressions)
                    if package_resource_license_expression:
                        discovered_license_expressions.append(package_resource_license_expression)
                if package_resource.holders:
                    discovered_holders.extend(h.get('value') for h in package_resource.holders)
            discovered_holders = process_holders(discovered_holders)

            combined_discovered_license_expression = combine_expressions(discovered_license_expressions)
            if combined_discovered_license_expression:
                simplified_discovered_license_expression = str(Licensing().parse(combined_discovered_license_expression).simplify())
            else:
                simplified_discovered_license_expression = None

            c = Consolidation(
                core_license_expression=package_license_expression,
                # Sort holders by holder key
                core_holders=[h for h, _ in sorted(copyright_summary.cluster(package_holders), key=lambda t: t[0].key)],
                other_license_expression=simplified_discovered_license_expression,
                # Sort holders by holder key
                other_holders=[h for h, _ in sorted(copyright_summary.cluster(discovered_holders), key=lambda t: t[0].key)],
                files_count=len([package_resource for package_resource in package_resources if package_resource.is_file]),
                resources=package_resources,
            )
            if is_build_file:
                c.identifier = package.name
                yield ConsolidatedComponent(
                    type='build',
                    consolidation=c
                )
            else:
                yield ConsolidatedPackage(
                    package=package,
                    consolidation=c
                )


def process_holders(holders):
    holders = [copyright_summary.Text(key=holder, original=holder) for holder in holders]

    for holder in holders:
        holder.normalize()

    holders = list(copyright_summary.filter_junk(holders))

    for holder in holders:
        holder.normalize()

    # keep non-empties
    holders = list(holder for holder in holders if holder.key)

    # convert to plain ASCII, then fingerprint
    for holder in holders:
        holder.transliterate()
        holder.fingerprint()

    # keep non-empties
    holders = list(holder for holder in holders if holder.key)
    return holders


def get_holders_consolidated_components(codebase):
    """
    Yield a ConsolidatedComponent for every directory if there are files with
    both license and copyright detected in them
    """
    if codebase.root.extra_data.get('in_package_component'):
        return

    # Step 1: Normalize license expressions and holders on file Resources and
    # save a list of holder keys that were detected in the immediate directory
    # on directory resources
    for resource in codebase.walk(topdown=False):
        # Each Resource we are processing is a directory
        if resource.is_file or resource.extra_data.get('in_package_component'):
            continue

        current_holders = set()
        for child in resource.children(codebase):
            # Each child we are processing is a file
            if (child.is_dir
                    or child.extra_data.get('in_package_component')
                    or (not child.license_expressions and not child.holders)):
                continue

            if child.license_expressions:
                license_expression = combine_expressions(child.license_expressions)
                if license_expression:
                    child.extra_data['normalized_license_expression'] = license_expression
                    child.save(codebase)

            if child.holders:
                holders = process_holders(h['value'] for h in child.holders)
                if holders:
                    # Dedupe holders
                    d = {}
                    for holder in holders:
                        if holder.key not in d:
                            d[holder.key] = holder
                    holders = [holder for _, holder in d.items()]

                    # Keep track of holders found in this immediate directory
                    for holder in holders:
                        if holder.key not in current_holders:
                            current_holders.add(holder.key)

                    child.extra_data['normalized_holders'] = holders
                    child.save(codebase)

        if current_holders:
            # Save a list of detected holders found in the immediate directory
            resource.extra_data['current_holders'] = current_holders
            resource.save(codebase)

    # Step 2: Walk the codebase top-down and create consolidated_components along the way.
    # By going top-down, we ensure that the highest-most Resource is used as the common
    # ancestor for a given holder.
    # We populate the `has_been_consolidated` set with the holder key to keep track of which
    # holders we have already created a consolidation for.
    has_been_consolidated = set()
    for resource in codebase.walk(topdown=True):
        for holder in resource.extra_data.get('current_holders', set()):
            if holder in has_been_consolidated:
                continue
            has_been_consolidated.add(holder)
            for c in create_consolidated_components(resource, codebase, holder):
                yield c


def create_consolidated_components(resource, codebase, holder_key):
    """
    Yield ConsolidatedComponents for every holder-grouped set of RIDs for a
    given resource and holder key
    """
    license_expressions = []
    holder = None
    resources = []
    for r in resource.walk(codebase):
        for normalized_holder in r.extra_data.get('normalized_holders', []):
            if not (normalized_holder.key == holder_key):
                continue
            normalized_license_expression = r.extra_data.get('normalized_license_expression')
            if normalized_license_expression:
                license_expressions.append(normalized_license_expression)
            if not holder:
                holder = normalized_holder
            resources.append(r)

    # We add the current directory Resource we are currently at to the set
    # of resources that have this particular key
    resources.append(resource)
    resource.extra_data['majority'] = True
    resource.save(codebase)

    c = Consolidation(
        core_license_expression=combine_expressions(license_expressions),
        core_holders=[holder],
        files_count=len([r for r in resources if r.is_file]),
        resources=resources,
    )
    yield ConsolidatedComponent(
        type='holders',
        consolidation=c
    )
