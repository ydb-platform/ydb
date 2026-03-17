#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import attr
import xmltodict

from packagedcode import models
from packagedcode.utils import build_description


# TODO: add dependencies

"""
Handle nuget.org Nuget packages.
"""

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
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


@attr.s()
class NugetPackage(models.Package):
    metafiles = ('[Content_Types].xml', '*.nuspec',)
    filetypes = ('zip archive', 'microsoft ooxml',)
    mimetypes = ('application/zip', 'application/octet-stream',)
    extensions = ('.nupkg',)

    default_type = 'nuget'
    default_web_baseurl = 'https://www.nuget.org/packages/'
    default_download_baseurl = 'https://www.nuget.org/api/v2/package/'
    default_api_baseurl = 'https://api.nuget.org/v3/registration3/'

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        if manifest_resource.name.endswith('.nupkg'):
            return manifest_resource
        if manifest_resource.name.endswith(('[Content_Types].xml', '.nuspec',)):
            return manifest_resource.parent(codebase)
        return manifest_resource

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        return baseurl + '{name}/{version}'.format(
            name=self.name, version=self.version)

    def repository_download_url(self, baseurl=default_download_baseurl):
        return baseurl + '{name}/{version}'.format(
            name=self.name, version=self.version)

    def api_data_url(self, baseurl=default_api_baseurl):
        # the name is lowercased
        # https://api.nuget.org/v3/registration3/newtonsoft.json/10.0.1.json
        return baseurl + '{name}/{version}.json'.format(
            name=self.name.lower(), version=self.version)


nuspec_tags = [
    'id',
    'version',
    'title',
    'authors',
    'owners',
    'licenseUrl',
    'projectUrl',
    'requireLicenseAcceptance',
    'description',
    'summary',
    'releaseNotes',
    'copyright',
    'repository/@type',
    'repository/@url',
]


def _parse_nuspec(location):
    """
    Return a dictionary of Nuget metadata from a .nuspec file at location.
    Return None if this is not a parsable nuspec.
    Raise Exceptions on errors.
    """
    if not location.endswith('.nuspec'):
        return
    with open(location , 'rb') as loc:
        return  xmltodict.parse(loc)


def parse(location):
    """
    Return a Nuget package from a nuspec XML file at `location`.
    Return None if this is not a parsable nuspec.
    """
    parsed = _parse_nuspec(location)
    if TRACE:
        logger_debug('parsed:', parsed)
    if not parsed:
        return

    pack = parsed.get('package', {}) or {}
    nuspec = pack.get('metadata')
    if not nuspec:
        return

    name=nuspec.get('id')
    version=nuspec.get('version')

    # Summary: A short description of the package for UI display. If omitted, a
    # truncated version of description is used.
    description = build_description(nuspec.get('summary') , nuspec.get('description'))

    # title: A human-friendly title of the package, typically used in UI
    # displays as on nuget.org and the Package Manager in Visual Studio. If not
    # specified, the package ID is used.
    title = nuspec.get('title')
    if title and title != name:
        description = build_description(nuspec.get('title') , description)

    parties = []
    authors = nuspec.get('authors')
    if authors:
        parties.append(models.Party(name=authors, role='author'))

    owners = nuspec.get('owners')
    if owners:
        parties.append(models.Party(name=owners, role='owner'))

    repo = nuspec.get('repository') or {}
    vcs_tool = repo.get('@type') or ''
    vcs_repository = repo.get('@url') or ''
    vcs_url =None
    if vcs_repository:
        if vcs_tool:
            vcs_url = '{}+{}'.format(vcs_tool, vcs_repository)
        else:
            vcs_url = vcs_repository

    package = NugetPackage(
        name=name,
        version=version,
        description=description or None,
        homepage_url=nuspec.get('projectUrl') or None,
        parties=parties,
        declared_license=nuspec.get('licenseUrl') or None,
        copyright=nuspec.get('copyright') or None,
        vcs_url=vcs_url,
    )
    return package
