#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import partial
import io
import json
import logging
import sys

import attr

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models
from packagedcode.utils import combine_expressions


"""
Parse PHP composer package manifests, see https://getcomposer.org/ and
https://packagist.org/

TODO: add support for composer.lock and packagist formats: both are fairly
similar.
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
class PHPComposerPackage(models.Package):
    metafiles = (
        'composer.json',
        'composer.lock',
    )
    extensions = ('.json', '.lock',)
    mimetypes = ('application/json',)

    default_type = 'composer'
    default_primary_language = 'PHP'
    default_web_baseurl = 'https://packagist.org'
    default_download_baseurl = None
    default_api_baseurl = 'https://packagist.org/p'

    @classmethod
    def recognize(cls, location):
        for package in parse(location):
            yield package

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        if self.namespace:
            return '{}/packages/{}/{}'.format(baseurl, self.namespace, self.name)
        else:
            return '{}/packages/{}'.format(baseurl, self.name)

    def api_data_url(self, baseurl=default_api_baseurl):
        if self.namespace:
            return '{}/packages/{}/{}.json'.format(baseurl, self.namespace, self.name)
        else:
            return '{}/packages/{}.json'.format(baseurl, self.name)

    def compute_normalized_license(self):
        """
        Per https://getcomposer.org/doc/04-schema.md#license this is an expression
        """
        return compute_normalized_license(self.declared_license)


def compute_normalized_license(declared_license):
    """
    Return a normalized license expression string detected from a list of
    declared license items or string type.
    """
    if not declared_license:
        return

    detected_licenses = []

    if isinstance(declared_license, str):
        if declared_license == 'proprietary':
            return declared_license
        if '(' in declared_license and ')' in declared_license and ' or ' in declared_license:
            declared_license = declared_license.strip().rstrip(')').lstrip('(')
            declared_license = declared_license.split(' or ')
        else:
            return models.compute_normalized_license(declared_license)

    if isinstance(declared_license, list):
        for declared in declared_license:
            detected_license = models.compute_normalized_license(declared)
            detected_licenses.append(detected_license)
    else:
        declared_license = repr(declared_license)
        detected_license = models.compute_normalized_license(declared_license)

    if detected_licenses:
        # build a proper license expression: the defaultfor composer is OR
        return combine_expressions(detected_licenses, 'OR')


def is_phpcomposer_json(location):
    return filetype.is_file(location) and fileutils.file_name(location).lower() == 'composer.json'


def is_phpcomposer_lock(location):
    return filetype.is_file(location) and fileutils.file_name(location).lower() == 'composer.lock'


def parse(location):
    """
    Yield Package objects from a composer.json or composer.lock file. Note that
    this is NOT exactly the packagist .json format (all are closely related of
    course but have important (even if minor) differences.
    """
    if is_phpcomposer_json(location):
        with io.open(location, encoding='utf-8') as loc:
            package_data = json.load(loc)
        yield build_package_from_json(package_data)

    elif is_phpcomposer_lock(location):
        with io.open(location, encoding='utf-8') as loc:
            package_data = json.load(loc)
        for package in build_packages_from_lock(package_data):
            yield package


def build_package_from_json(package_data):
    """
    Return a composer Package object from a package data mapping or None.
    """
    # A composer.json without name and description is not a usable PHP
    # composer package. Name and description fields are required but
    # only for published packages:
    # https://getcomposer.org/doc/04-schema.md#name
    # We want to catch both published and non-published packages here.
    # Therefore, we use "private-package-without-a-name" as a package name if
    # there is no name.

    ns_name = package_data.get('name')
    is_private = False
    if not ns_name:
        ns = None
        name = 'private-package-without-a-name'
        is_private = True
    else:
        ns, _, name = ns_name.rpartition('/')

    package = PHPComposerPackage(
        namespace=ns,
        name=name,
    )

    # mapping of top level composer.json items to the Package object field name
    plain_fields = [
        ('version', 'version'),
        ('description', 'summary'),
        ('keywords', 'keywords'),
        ('homepage', 'homepage_url'),
    ]

    for source, target in plain_fields:
        value = package_data.get(source)
        if isinstance(value, str):
            value = value.strip()
            if value:
                setattr(package, target, value)

    # mapping of top level composer.json items to a function accepting as
    # arguments the composer.json element value and returning an iterable of
    # key, values Package Object to update
    field_mappers = [
        ('authors', author_mapper),
        ('license', partial(licensing_mapper, is_private=is_private)),
        ('support', support_mapper),
        ('require', partial(_deps_mapper, scope='require', is_runtime=True)),
        ('require-dev', partial(_deps_mapper, scope='require-dev', is_optional=True)),
        ('provide', partial(_deps_mapper, scope='provide', is_runtime=True)),
        ('conflict', partial(_deps_mapper, scope='conflict', is_runtime=True, is_optional=True)),
        ('replace', partial(_deps_mapper, scope='replace', is_runtime=True, is_optional=True)),
        ('suggest', partial(_deps_mapper, scope='suggest', is_runtime=True, is_optional=True)),
        ('source', source_mapper),
        ('dist', dist_mapper)
    ]

    for source, func in field_mappers:
        logger.debug('parse: %(source)r, %(func)r' % locals())
        value = package_data.get(source)
        if value:
            if isinstance(value, str):
                value = value.strip()
            if value:
                func(value, package)
    # Parse vendor from name value
    vendor_mapper(package)
    return package


def licensing_mapper(licenses, package, is_private=False):
    """
    Update package licensing and return package.
    Licensing data structure has evolved over time and is a tad messy.
    https://getcomposer.org/doc/04-schema.md#license
    The value of license is either:
    - an SPDX expression string:  {  "license": "(LGPL-2.1 or GPL-3.0+)" }
    - a list of SPDX license ids choices: "license": ["LGPL-2.1","GPL-3.0+"]

    Some older licenses are plain strings and not SPDX ids. Also if there is no
    license and the `is_private` Fkag is True, we return a "proprietary-license"
    license.
    """
    if not licenses and is_private:
        package.declared_license = 'proprietary-license'
        return package

    package.declared_license = licenses
    return package


def author_mapper(authors_content, package):
    """
    Update package parties with authors and return package.
    https://getcomposer.org/doc/04-schema.md#authors
    """
    for name, role, email, url in parse_person(authors_content):
        role = role or 'author'
        package.parties.append(
            models.Party(type=models.party_person, name=name,
                         role=role, email=email, url=url))
    return package


def support_mapper(support, package):
    """
    Update support and bug tracking url.
    https://getcomposer.org/doc/04-schema.md#support
    """
    # TODO: there are many other information we ignore for now
    package.bug_tracking_url = support.get('issues') or None
    package.code_view_url = support.get('source') or None
    return package


def source_mapper(source, package):
    """
    Add vcs_url from source tag, if present. Typically only present in
    composer.lock
    """
    tool = source.get('type')
    if not tool:
        return package
    url = source.get('url')
    if not url:
        return package
    version = source.get('reference')
    package.vcs_url = '{tool}+{url}@{version}'.format(**locals())
    return package


def dist_mapper(dist, package):
    """
    Add download_url from source tag, if present. Typically only present in
    composer.lock
    """
    url = dist.get('url')
    if not url:
        return package
    package.download_url = url
    return package


def vendor_mapper(package):
    """
    Vendor is the first part of the name element.
    https://getcomposer.org/doc/04-schema.md#name
    """
    if package.namespace:
        package.parties.append(
            models.Party(type=models.party_person,
                         name=package.namespace, role='vendor'))
    return package


def _deps_mapper(deps, package, scope, is_runtime=False, is_optional=False):
    """
    Handle deps such as dependencies, devDependencies
    return a tuple of (dep type, list of deps)
    https://getcomposer.org/doc/04-schema.md#package-links
    """
    for ns_name, requirement in deps.items():
        ns, _, name = ns_name.rpartition('/')
        purl = models.PackageURL(type='composer', namespace=ns, name=name).to_string()
        dep = models.DependentPackage(
            purl=purl,
            requirement=requirement,
            scope=scope,
            is_runtime=is_runtime,
            is_optional=is_optional)
        package.dependencies.append(dep)
    return package


def parse_person(persons):
    """
    https://getcomposer.org/doc/04-schema.md#authors
    A "person" is an object with a "name" field and optionally "url" and "email".

    Yield  a name, email, url tuple for a person object
    A person can be in the form:
        "authors": [
            {
                "name": "Nils Adermann",
                "email": "naderman@naderman.de",
                "homepage": "http://www.naderman.de",
                "role": "Developer"
            },
            {
                "name": "Jordi Boggiano",
                "email": "j.boggiano@seld.be",
                "homepage": "http://seld.be",
                "role": "Developer"
            }
        ]

    Both forms are equivalent.
    """
    if isinstance(persons, list):
        for person in persons:
            # ensure we have our three values
            name = person.get('name')
            role = person.get('role')
            email = person.get('email')
            url = person.get('homepage')
            # FIXME: this got cargoculted from npm package.json parsing
            yield (
                name and name.strip(),
                role and role.strip(),
                email and email.strip('<> '),
                url and url.strip('() '))
    else:
        raise ValueError('Incorrect PHP composer persons: %(persons)r' % locals())


def build_dep_package(package, scope, is_runtime, is_optional):
    return models.DependentPackage(
        purl=package.purl,
        scope=scope,
        is_runtime=is_runtime,
        is_optional=is_optional,
        is_resolved=True,
    )


def build_packages_from_lock(package_data):
    """
    Yield composer Package objects from a package data mapping that originated
    from a composer.lock file
    """
    packages = [build_package_from_json(p) for p in package_data.get('packages', [])]
    packages_dev = [build_package_from_json(p) for p in package_data.get('packages-dev', [])]
    required_deps = [build_dep_package(p, scope='require', is_runtime=True, is_optional=False) for p in packages]
    required_dev_deps = [build_dep_package(p, scope='require-dev', is_runtime=False, is_optional=True) for p in packages_dev]
    yield PHPComposerPackage(dependencies=required_deps + required_dev_deps)
    for package in packages + packages_dev:
        yield package
