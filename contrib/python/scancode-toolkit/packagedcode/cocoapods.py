#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import re

import attr

from commoncode import filetype
from packagedcode import models
from packagedcode.spec import Spec

"""
Handle cocoapods packages manifests for macOS and iOS
including .podspec, Podfile and Podfile.lock files.
See https://cocoapods.org
"""

# TODO: override the license detection to detect declared_license correctly.

TRACE = False

logger = logging.getLogger(__name__)

if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)


@attr.s()
class CocoapodsPackage(models.Package):
    metafiles = ('*.podspec',)
    extensions = ('.podspec',)
    default_type = 'pods'
    default_primary_language = 'Objective-C'
    default_web_baseurl = 'https://cocoapods.org'
    default_download_baseurl = None
    default_api_baseurl = None

    @classmethod
    def recognize(cls, location):
        yield parse(location)

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        return '{}/pods/{}'.format(baseurl, self.name)

    def repository_download_url(self):
        return '{}/archive/{}.zip'.format(self.homepage_url, self.version)


def is_podspec(location):
    """
    Checks if the file is actually a podspec file
    """
    return (filetype.is_file(location) and location.endswith('.podspec'))


def parse(location):
    """
    Return a Package object from a .podspec file or None.
    """
    if not is_podspec(location):
        return

    podspec_object = Spec()
    podspec_data = podspec_object.parse_spec(location)
    return build_package(podspec_data)


def build_package(podspec_data):
    """
    Return a Package object from a package data mapping or None.
    """
    name = podspec_data.get('name')
    version = podspec_data.get('version')
    declared_license = podspec_data.get('license')
    summary = podspec_data.get('summary', '')
    description = podspec_data.get('description', '')
    homepage_url = podspec_data.get('homepage_url')
    source = podspec_data.get('source')
    authors = podspec_data.get('author') or []
    if summary and not description.startswith(summary):
        desc = [summary]
        if description:
            desc += [description]
        description = '\n'.join(desc)

    author_names = []
    author_email = []
    if authors:
        for split_author in authors:
            split_author = split_author.strip()
            author, email = parse_person(split_author)
            author_names.append(author)
            author_email.append(email)

    parties = list(party_mapper(author_names, author_email))

    package = CocoapodsPackage(
        name=name,
        version=version,
        vcs_url=source,
        source_packages=list(source.split('\n')),
        description=description,
        declared_license=declared_license,
        homepage_url=homepage_url,
        parties=parties
    )

    return package


def party_mapper(author, email):
    """
    Yields a Party object with author and email.
    """
    for person in author:
        yield models.Party(
            type=models.party_person,
            name=person,
            role='author')

    for person in email:
        yield models.Party(
            type=models.party_person,
            email=person,
            role='email')


person_parser = re.compile(
    r'^(?P<name>[\w\s(),-_.,]+)'
    r'=>'
    r'(?P<email>[\S+]+$)'
).match

person_parser_only_name = re.compile(
    r'^(?P<name>[\w\s(),-_.,]+)'
).match


def parse_person(person):
    """
    Return name and email from person string.

    https://guides.cocoapods.org/syntax/podspec.html#authors
    Author can be in the form:
        s.author = 'Rohit Potter'
        or
        s.author = 'Rohit Potter=>rohit@gmail.com'
    Author check:
    >>> p = parse_person('Rohit Potter=>rohit@gmail.com')
    >>> assert p == ('Rohit Potter', 'rohit@gmail.com')
    >>> p = parse_person('Rohit Potter')
    >>> assert p == ('Rohit Potter', None)
    """
    parsed = person_parser(person)
    if not parsed:
        parsed = person_parser_only_name(person)
        name = parsed.group('name')
        email = None
    else:
        name = parsed.group('name')
        email = parsed.group('email')

    return name, email
