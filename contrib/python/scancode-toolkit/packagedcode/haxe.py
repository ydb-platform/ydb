
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import io
import json
import logging

import attr
from packageurl import PackageURL

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models


"""
Handle haxelib Haxe packages

See
- https://lib.haxe.org/all/ this lists all the packages.
- https://lib.haxe.org/documentation/creating-a-haxelib-package/
- https://github.com/HaxeFoundation/haxelib
- https://github.com/gogoprog/hxsocketio/blob/master/haxelib.json
- https://github.com/HaxeFoundation/haxelib/blob/development/haxelib.json

Download and homepage are using these conventions:
- https://lib.haxe.org/p/format/
- https://lib.haxe.org/files/3.0/tweenx-1,0,4.zip
- https://lib.haxe.org/p/format/3.4.1/download/
- https://lib.haxe.org/files/3.0/format-3,4,1.zip
"""


TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


@attr.s()
class HaxePackage(models.Package):
    metafiles = ('haxelib.json',)
    filetypes = tuple()
    mimetypes = tuple()
    default_type = 'haxe'
    default_primary_language = 'Haxe'
    default_web_baseurl = 'https://lib.haxe.org/p/'
    default_download_baseurl = 'https://lib.haxe.org/p/'

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        return haxelib_homepage_url(self.name, baseurl=baseurl)

    def repository_download_url(self, baseurl=default_download_baseurl):
        return haxelib_download_url(self.name, self.version, baseurl=baseurl)


def haxelib_homepage_url(name, baseurl='https://lib.haxe.org/p/'):
    """
    Return an haxelib package homepage URL given a name and a base registry web
    interface URL.

    For example:
    >>> assert haxelib_homepage_url('format') == u'https://lib.haxe.org/p/format'
    """
    baseurl = baseurl.rstrip('/')
    return '{baseurl}/{name}'.format(**locals())


def haxelib_download_url(name, version, baseurl='https://lib.haxe.org/p'):
    """
    Return an haxelib package tarball download URL given a namespace, name, version
    and a base registry URL.

    For example:
    >>> assert haxelib_download_url('format', '3.4.1') == u'https://lib.haxe.org/p/format/3.4.1/download/'
    """
    if name and version:
        baseurl = baseurl.rstrip('/')
        return '{baseurl}/{name}/{version}/download/'.format(**locals())


def is_haxelib_json(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'haxelib.json')


def parse(location):
    """
    Return a Package object from a haxelib.json file or None.
    """
    if not is_haxelib_json(location):
        return

    with io.open(location, encoding='utf-8') as loc:
        package_data = json.load(loc)
    return build_package(package_data)


def build_package(package_data):
    """
    Return a Package object from a package_data mapping (from a
    haxelib.json or similar) or None.
    {
        "name": "haxelib",
        "url" : "https://lib.haxe.org/documentation/",
        "license": "GPL",
        "tags": ["haxelib", "core"],
        "description": "The haxelib client",
        "classPath": "src",
        "version": "3.4.0",
        "releasenote": " * Fix password input issue in Windows (#421).\n * ....",
        "contributors": ["back2dos", "ncannasse", "jason", "Simn", "nadako", "andyli"]
    }

    """
    package = HaxePackage(
        name=package_data.get('name'),
        version=package_data.get('version'),
        homepage_url=package_data.get('url'),
        declared_license=package_data.get('license'),
        keywords=package_data.get('tags'),
        description=package_data.get('description'),
    )

    package.download_url = package.repository_download_url()

    for contrib in package_data.get('contributors', []):
        party = models.Party(
            type=models.party_person,
            name=contrib,
            role='contributor',
            url='https://lib.haxe.org/u/{}'.format(contrib))
        package.parties.append(party)

    for dep_name, dep_version in package_data.get('dependencies', {}).items():
        dep_version = dep_version and dep_version.strip()
        is_resolved = bool(dep_version)
        dep_purl = PackageURL(
            type='haxe',
            name=dep_name,
            version=dep_version
        ).to_string()
        dep = models.DependentPackage(purl=dep_purl, is_resolved=is_resolved,)
        package.dependencies.append(dep)

    return package


def map_license(package):
    """
    Update the license based on a mapping:
    Per the doc:
    Can be GPL, LGPL, BSD, Public (for Public Domain), MIT, or Apache.
    """
    raise NotImplementedError
