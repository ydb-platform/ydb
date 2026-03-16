#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import defaultdict
import ast
import logging
import os

import attr

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models
from packagedcode.utils import combine_expressions
from scancode.api import get_licenses


TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


"""
Detect as Packages common build tools and environment such as Make, Autotools,
gradle, Buck, Bazel, Pants, etc.
"""


@attr.s()
class BaseBuildManifestPackage(models.Package):
    metafiles = tuple()

    @classmethod
    def recognize(cls, location):
        if not cls._is_build_manifest(location):
            return

        # we use the parent directory as a name
        name = fileutils.file_name(fileutils.parent_directory(location))
        # we could use checksums as version in the future
        version = None

        # there is an optional array of license file names in targets that we could use
        # declared_license = None
        # there is are dependencies we could use
        # dependencies = []
        yield cls(
            name=name,
            version=version)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)

    @classmethod
    def _is_build_manifest(cls, location):
        if not filetype.is_file(location):
            return False
        fn = fileutils.file_name(location)
        return any(fn == mf for mf in cls.metafiles)


@attr.s()
class AutotoolsPackage(BaseBuildManifestPackage):
    metafiles = ('configure', 'configure.ac',)
    default_type = 'autotools'


starlark_rule_types = [
    'binary',
    'library'
]


@attr.s()
class StarlarkManifestPackage(BaseBuildManifestPackage):
    @classmethod
    def recognize(cls, location):
        if not cls._is_build_manifest(location):
            return

        # Thanks to Starlark being a Python dialect, we can use the `ast`
        # library to parse it
        with open(location, 'rb') as f:
            tree = ast.parse(f.read())

        build_rules = defaultdict(list)
        for statement in tree.body:
            # We only care about function calls or assignments to functions whose
            # names ends with one of the strings in `rule_types`
            if (isinstance(statement, ast.Expr)
                    or isinstance(statement, ast.Call)
                    or isinstance(statement, ast.Assign)
                    and isinstance(statement.value, ast.Call)
                    and isinstance(statement.value.func, ast.Name)
                    and statement.value.func.id.endswith(starlark_rule_types)):
                rule_name = statement.value.func.id
                # Process the rule arguments
                args = {}
                for kw in statement.value.keywords:
                    arg_name = kw.arg
                    if isinstance(kw.value, ast.Str):
                        args[arg_name] = kw.value.s
                    if isinstance(kw.value, ast.List):
                        # We collect the elements of a list if the element is not a function call
                        args[arg_name] = [elt.s for elt in kw.value.elts if not isinstance(elt, ast.Call)]
                if args:
                    build_rules[rule_name].append(args)

        if build_rules:
            for rule_name, rule_instances_args in build_rules.items():
                for args in rule_instances_args:
                    name = args.get('name')
                    if not name:
                        continue
                    license_files = args.get('licenses')
                    yield cls(
                        name=name,
                        declared_license=license_files,
                        root_path=fileutils.parent_directory(location)
                    )
        else:
            # If we don't find anything in the manifest file, we yield a Package with
            # the parent directory as the name
            yield cls(
                name=fileutils.file_name(fileutils.parent_directory(location))
            )

    def compute_normalized_license(self):
        """
        Return a normalized license expression string detected from a list of
        declared license items.
        """
        declared_license = self.declared_license
        manifest_parent_path = self.root_path

        if not declared_license or not manifest_parent_path:
            return

        license_expressions = []
        for license_file in declared_license:
            license_file_path = os.path.join(manifest_parent_path, license_file)
            if os.path.exists(license_file_path) and os.path.isfile(license_file_path):
                licenses = get_licenses(license_file_path)
                license_expressions.extend(licenses.get('license_expressions', []))

        return combine_expressions(license_expressions)


@attr.s()
class BazelPackage(StarlarkManifestPackage):
    metafiles = ('BUILD',)
    default_type = 'bazel'


@attr.s()
class BuckPackage(StarlarkManifestPackage):
    metafiles = ('BUCK',)
    default_type = 'buck'


@attr.s()
class MetadataBzl(BaseBuildManifestPackage):
    metafiles = ('METADATA.bzl',)
    # TODO: Not sure what the default type should be, change this to something
    # more appropriate later
    default_type = 'METADATA.bzl'

    @classmethod
    def recognize(cls, location):
        if not cls._is_build_manifest(location):
            return

        with open(location, 'rb') as f:
            tree = ast.parse(f.read())

        metadata_fields = {}
        for statement in tree.body:
            if not (hasattr(statement, 'targets') and isinstance(statement, ast.Assign)):
                continue
            # We are looking for a dictionary assigned to the variable `METADATA`
            for target in statement.targets:
                if not (target.id == 'METADATA' and isinstance(statement.value, ast.Dict)):
                    continue
                # Once we find the dictionary assignment, get and store its contents
                statement_keys = statement.value.keys
                statement_values = statement.value.values
                for statement_k, statement_v in zip(statement_keys, statement_values):
                    if isinstance(statement_k, ast.Str):
                        key_name = statement_k.s
                    # The list values in a `METADATA.bzl` file seem to only contain strings
                    if isinstance(statement_v, ast.List):
                        value = []
                        for e in statement_v.elts:
                            if not isinstance(e, ast.Str):
                                continue
                            value.append(e.s)
                    if isinstance(statement_v, ast.Str):
                        value = statement_v.s
                    metadata_fields[key_name] = value

        parties = []
        maintainers = metadata_fields.get('maintainers', [])
        for maintainer in maintainers:
            parties.append(
                models.Party(
                    type=models.party_org,
                    name=maintainer,
                    role='maintainer',
                )
            )

        # TODO: Create function that determines package type from download URL,
        # then create a package of that package type from the metadata info
        yield cls(
            type=metadata_fields.get('upstream_type', ''),
            name=metadata_fields.get('name', ''),
            version=metadata_fields.get('version', ''),
            declared_license=metadata_fields.get('licenses', []),
            parties=parties,
            homepage_url=metadata_fields.get('upstream_address', ''),
            # TODO: Store 'upstream_hash` somewhere
        )

    def compute_normalized_license(self):
        """
        Return a normalized license expression string detected from a list of
        declared license strings.
        """
        if not self.declared_license:
            return

        detected_licenses = []
        for declared in self.declared_license:
            detected_license = models.compute_normalized_license(declared)
            detected_licenses.append(detected_license)

        if detected_licenses:
            return combine_expressions(detected_licenses)
