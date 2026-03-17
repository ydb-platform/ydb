#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import logging
import sys

import attr
import saneyaml

from commoncode import filetype
from commoncode import fileutils

from packagedcode import models
from packageurl import PackageURL


"""
Parse Conda manifests, see https://docs.conda.io/en/latest/

"""

TRACE = False


def logger_debug(*args):
    pass


logger = logging.getLogger(__name__)

if TRACE:
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


@attr.s()
class CondaPackage(models.Package):
    metafiles = ('meta.yaml', 'META.yml',)
    default_type = 'conda'
    default_web_baseurl = None
    default_download_baseurl = 'https://repo.continuum.io/pkgs/free'
    default_api_baseurl = None

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        if manifest_resource.name.lower().endswith(('.yaml', '.yml')):
            # the root is either the parent or further up for yaml stored under
            # a INFO dir
            path = 'info/recipe.tar-extract/recipe/meta.yaml'
            if manifest_resource.path.endswith(path):
                for ancestor in manifest_resource.ancestors(codebase):
                    if ancestor.name == 'info':
                        root_dir = ancestor.parent(codebase)
                        return root_dir
            return manifest_resource.parent(codebase)
        else:
            return manifest_resource

    @classmethod
    def extra_root_dirs(cls):
        return ['**/recipe.tar-extract/recipe']

    def compute_normalized_license(self):
        return models.compute_normalized_license(self.declared_license)


def is_conda_yaml(location):
    return (filetype.is_file(location) and fileutils.file_name(location).lower().endswith(('.yaml', '.yml')))


def parse(location):
    """
    Return a Package object from a meta.yaml file or None.
    """
    if not is_conda_yaml(location):
        return

    yaml_data = get_yaml_data(location)
    return build_package(yaml_data)


def build_package(package_data):
    """
    Return a Conda Package object from a dictionary yaml data.
    """
    name = None
    version = None

    # Handle the package element
    package_element = package_data.get('package')
    if package_element:
        for key, value in package_element.items():
            if key == 'name':
                name = value
            elif key == 'version':
                version = value
    if not name:
        return

    package = CondaPackage(
        name=name,
        version=version or None,
    )

    # Handle the source element
    source_element = package_data.get('source')
    if source_element:
        for key, value in source_element.items():
            if key == 'url' and value:
                package.download_url = value
            elif key == 'sha256' and value:
                package.sha256 = value

    # Handle the about element
    about_element = package_data.get('about')
    if about_element:
        for key, value in about_element.items():
            if key == 'home' and value:
                package.homepage_url = value
            elif key == 'license' and value:
                package.declared_license = value
            elif key == 'summary' and value:
                package.description = value
            elif key == 'dev_url' and value:
                package.vcs_url = value

    # Handle the about element
    requirements_element = package_data.get('requirements')
    if requirements_element:
        for key, value in requirements_element.items():
            # Run element format is like:
            # (u'run', [u'mccortex ==1.0', u'nextflow ==19.01.0', u'cortexpy ==0.45.7', u'kallisto ==0.44.0', u'bwa', u'pandas', u'progressbar2', u'python >=3.6'])])
            if key == 'run' and value and isinstance(value, (list, tuple)):
                package_dependencies = []
                for dependency in value:
                    requirement = None
                    for splitter in ('==', '>=', '<=', '>', '<'):
                        if splitter in dependency:
                            splits = dependency.split(splitter)
                            # Replace the package name and keep the relationship and version
                            # For example: keep ==19.01.0
                            requirement = dependency.replace(splits[0], '').strip()
                            dependency = splits[0].strip()
                            break
                    package_dependencies.append(
                        models.DependentPackage(
                            purl=PackageURL(
                                type='conda', name=dependency).to_string(),
                            requirement=requirement,
                            scope='dependencies',
                            is_runtime=True,
                            is_optional=False,
                        )
                    )
                package.dependencies = package_dependencies
    return package


def get_yaml_data(location):
    """
    Get variables and parse the yaml file, replace the variable with the value and return dictionary.
    """
    variables = get_variables(location)
    yaml_lines = []
    with io.open(location, encoding='utf-8') as loc:
        for line in loc.readlines():
            if not line:
                continue
            pure_line = line.strip()
            if pure_line.startswith('{%') and pure_line.endswith('%}') and '=' in pure_line:
                continue
            # Replace the variable with the value
            if '{{' in line and '}}' in line:
                for variable, value in variables.items():
                    line = line.replace('{{ ' + variable + ' }}', value)
            yaml_lines.append(line)
    return saneyaml.load('\n'.join(yaml_lines))


def get_variables(location):
    """
    Conda yaml will have variables defined at the beginning of the file, the idea is to parse it and return a dictionary of the variable and value
    For example:
    {% set version = "0.45.0" %}
    {% set sha256 = "bc7512f2eef785b037d836f4cc6faded457ac277f75c6e34eccd12da7c85258f" %}
    """
    result = {}
    with io.open(location, encoding='utf-8') as loc:
        for line in loc.readlines():
            if not line:
                continue
            line = line.strip()
            if line.startswith('{%') and line.endswith('%}') and '=' in line:
                line = line.lstrip('{%').rstrip('%}').strip().lstrip('set').lstrip()
                parts = line.split('=')
                result[parts[0].strip()] = parts[-1].strip().strip('"')
    return result
