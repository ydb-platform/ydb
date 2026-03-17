
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import base64
from collections import defaultdict
from functools import partial
import io
import json
import logging
import re

import attr
from packageurl import PackageURL

from commoncode import filetype
from commoncode import fileutils
from packagedcode import models
from packagedcode.utils import combine_expressions
from packagedcode.utils import normalize_vcs_url

"""
Handle Node.js npm packages
per https://docs.npmjs.com/files/package.json
"""

"""
To check https://github.com/npm/normalize-package-data
"""

TRACE = False

logger = logging.getLogger(__name__)


def logger_debug(*args):
    pass


if TRACE:
    import sys
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return print(' '.join(isinstance(a, str) and a or repr(a) for a in args))

# TODO: add os and engines from package.json??
# add lock files and yarn details


@attr.s()
class NpmPackage(models.Package):
    # TODO: add new lock files and yarn lock files
    metafiles = (
        'package.json',
        'npm-shrinkwrap.json',
        'package-lock.json',
        'yarn.lock',
    )
    extensions = ('.tgz',)
    mimetypes = ('application/x-tar',)
    default_type = 'npm'
    default_primary_language = 'JavaScript'
    default_web_baseurl = 'https://www.npmjs.com/package'
    default_download_baseurl = 'https://registry.npmjs.org'
    default_api_baseurl = 'https://registry.npmjs.org'

    @classmethod
    def recognize(cls, location):
        for package in parse(location):
            yield package

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)

    @classmethod
    def ignore_resource(cls, resource, codebase):
        return resource.is_dir and resource.name == 'node_modules'

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        return npm_homepage_url(self.namespace, self.name, registry=baseurl)

    def repository_download_url(self, baseurl=default_download_baseurl):
        return npm_download_url(self.namespace, self.name, self.version, registry=baseurl)

    def api_data_url(self, baseurl=default_api_baseurl):
        return npm_api_url(self.namespace, self.name, self.version, registry=baseurl)

    def compute_normalized_license(self):
        return compute_normalized_license(self.declared_license)


def compute_normalized_license(declared_license):
    """
    Return a normalized license expression string detected from a list of
    declared license items.
    """
    if not declared_license:
        return

    detected_licenses = []

    for declared in declared_license:
        if isinstance(declared, str):
            detected_license = models.compute_normalized_license(declared)
            if detected_license:
                detected_licenses.append(detected_license)

        elif isinstance(declared, dict):
            # 1. try detection on the value of type if not empty and keep this
            ltype = declared.get('type')
            via_type = models.compute_normalized_license(ltype)

            # 2. try detection on the value of url  if not empty and keep this
            url = declared.get('url')
            via_url = models.compute_normalized_license(url)

            if via_type:
                # The type should have precedence and any unknowns
                # in url should be ignored.
                # TODO: find a better way to detect unknown licenses
                if via_url in ('unknown', 'unknown-license-reference',):
                    via_url = None

            if via_type:
                if via_type == via_url:
                    detected_licenses.append(via_type)
                else:
                    if not via_url:
                        detected_licenses.append(via_type)
                    else:
                        combined_expression = combine_expressions([via_type, via_url])
                        detected_licenses.append(combined_expression)
            elif via_url:
                detected_licenses.append(via_url)

    if detected_licenses:
        return combine_expressions(detected_licenses)


def npm_homepage_url(namespace, name, registry='https://www.npmjs.com/package'):
    """
    Return an npm package registry homepage URL given a namespace, name,
    version and a base registry web interface URL.

    For example:
    >>> assert npm_homepage_url('@invisionag', 'eslint-config-ivx') == 'https://www.npmjs.com/package/@invisionag/eslint-config-ivx'
    >>> assert npm_homepage_url(None, 'angular') == 'https://www.npmjs.com/package/angular'
    >>> assert npm_homepage_url('', 'angular') == 'https://www.npmjs.com/package/angular'
    >>> assert npm_homepage_url('', 'angular', 'https://yarnpkg.com/en/package/') == 'https://yarnpkg.com/en/package/angular'
    >>> assert npm_homepage_url('@ang', 'angular', 'https://yarnpkg.com/en/package') == 'https://yarnpkg.com/en/package/@ang/angular'
    """
    registry = registry.rstrip('/')

    if namespace:
        ns_name = '/'.join([namespace, name])
    else:
        ns_name = name
    return '%(registry)s/%(ns_name)s' % locals()


def npm_download_url(namespace, name, version, registry='https://registry.npmjs.org'):
    """
    Return an npm package tarball download URL given a namespace, name, version
    and a base registry URL.

    For example:
    >>> assert npm_download_url('@invisionag', 'eslint-config-ivx', '0.1.4') == 'https://registry.npmjs.org/@invisionag/eslint-config-ivx/-/eslint-config-ivx-0.1.4.tgz'
    >>> assert npm_download_url('', 'angular', '1.6.6') == 'https://registry.npmjs.org/angular/-/angular-1.6.6.tgz'
    >>> assert npm_download_url(None, 'angular', '1.6.6') == 'https://registry.npmjs.org/angular/-/angular-1.6.6.tgz'
    """
    registry = registry.rstrip('/')
    if namespace:
        ns_name = '/'.join([namespace, name])
    else:
        ns_name = name
    return '{registry}/{ns_name}/-/{name}-{version}.tgz'.format(**locals())


def npm_api_url(namespace, name, version=None, registry='https://registry.npmjs.org'):
    """
    Return a package API data URL given a namespace, name, version and a base
    registry URL.

    Note that for scoped packages (with a namespace), the URL is not version
    specific but contains the data for all versions as the default behvior of
    the registries is to return nothing in this case. Special quoting rules are
    applied for scoped npms.

    For example:
    >>> x = npm_api_url(
    ... '@invisionag', 'eslint-config-ivx', '0.1.4',
    ... 'https://registry.yarnpkg.com')
    >>> assert x == 'https://registry.yarnpkg.com/@invisionag%2feslint-config-ivx'
    >>> assert npm_api_url(None, 'angular', '1.6.6') == 'https://registry.npmjs.org/angular/1.6.6'
    """
    registry = registry.rstrip('/')
    version = version or ''
    if namespace:
        ns_name = '%2f'.join([namespace, name])
        # there is no version-specific URL for scoped packages
        version = ''
    else:
        ns_name = name

    if version:
        version = '/' + version
    return '{registry}/{ns_name}{version}'.format(**locals())


def is_package_json(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'package.json')


def is_package_lock(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'package-lock.json')


def is_npm_shrinkwrap(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'npm-shrinkwrap.json')


def is_yarn_lock(location):
    return (filetype.is_file(location)
            and fileutils.file_name(location).lower() == 'yarn.lock')


def parse(location):
    """
    Return a Package object from a package.json file or None.
    """
    if is_package_json(location):
        with io.open(location, encoding='utf-8') as loc:
            package_data = json.load(loc)
        yield build_package(package_data)

    if is_package_lock(location) or is_npm_shrinkwrap(location):
        with io.open(location, encoding='utf-8') as loc:
            package_data = json.load(loc)
        for package in build_packages_from_lockfile(package_data):
            yield package

    if is_yarn_lock(location):
        with io.open(location, encoding='utf-8') as loc:
            for package in build_packages_from_yarn_lock(loc):
                yield package


def build_package(package_data):
    """
    Return a Package object from a package_data mapping (from a package.json or
    similar) or None.
    """

    name = package_data.get('name')
    version = package_data.get('version')
    homepage = package_data.get('homepage', '')

    if not name:
        # a package.json without name and version is not a usable npm package
        # FIXME: raise error?
        return

    if isinstance(homepage, list):
        if homepage:
            homepage = homepage[0]
        else:
            homepage = ''
    namespace, name = split_scoped_package_name(name)
    package = NpmPackage(
        namespace=namespace or None,
        name=name,
        version=version or None,
        description=package_data.get('description', '').strip() or None,
        homepage_url=homepage.strip() or None,
    )
    vcs_revision = package_data.get('gitHead') or None

    # mapping of top level package.json items to a function accepting as
    # arguments the package.json element value and returning an iterable of (key,
    # values) to update on a package
    field_mappers = [
        ('author', partial(party_mapper, party_type='author')),
        ('contributors', partial(party_mapper, party_type='contributor')),
        ('maintainers', partial(party_mapper, party_type='maintainer')),

        ('dependencies', partial(deps_mapper, field_name='dependencies')),
        ('devDependencies', partial(deps_mapper, field_name='devDependencies')),
        ('peerDependencies', partial(deps_mapper, field_name='peerDependencies')),
        ('optionalDependencies', partial(deps_mapper, field_name='optionalDependencies')),
        ('bundledDependencies', bundle_deps_mapper),
        ('repository', partial(vcs_repository_mapper, vcs_revision=vcs_revision)),
        ('keywords', keywords_mapper,),
        ('bugs', bugs_mapper),
        ('dist', dist_mapper),
    ]

    for source, func in field_mappers:
        if TRACE: logger.debug('parse: %(source)r, %(func)r' % locals())
        value = package_data.get(source) or None
        if value:
            if isinstance(value, str):
                value = value.strip()
            if value:
                func(value, package)

    if not package.download_url:
        # Only add a synthetic download URL if there is none from the dist mapping.
        dnl_url = npm_download_url(package.namespace, package.name, package.version)
        package.download_url = dnl_url.strip()

    # licenses are a tad special with many different data structures
    lic = package_data.get('license')
    lics = package_data.get('licenses')
    package = licenses_mapper(lic, lics, package)
    if TRACE:
        declared_license = package.declared_license
        logger.debug(
            'parse: license: {lic} licenses: {lics} '
            'declared_license: {declared_license}'.format(locals()))

    return package


def is_scoped_package(name):
    """
    Return True if name contains a namespace.

    For example::
    >>> is_scoped_package('@angular')
    True
    >>> is_scoped_package('some@angular')
    False
    >>> is_scoped_package('linq')
    False
    >>> is_scoped_package('%40angular')
    True
    """
    return name.startswith(('@', '%40',))


def split_scoped_package_name(name):
    """
    Return a tuple of (namespace, name) given a package name.
    Namespace is the "scope" of a scoped package.
    / and @ can be url-quoted and will be unquoted.

    For example:
    >>> nsn = split_scoped_package_name('@linclark/pkg')
    >>> assert ('@linclark', 'pkg') == nsn, nsn
    >>> nsn = split_scoped_package_name('@linclark%2fpkg')
    >>> assert ('@linclark', 'pkg') == nsn, nsn
    >>> nsn = split_scoped_package_name('angular')
    >>> assert (None, 'angular') == nsn, nsn
    >>> nsn = split_scoped_package_name('%40angular%2fthat')
    >>> assert ('@angular', 'that') == nsn, nsn
    >>> nsn = split_scoped_package_name('%40angular')
    >>> assert ('@angular', None) == nsn, nsn
    >>> nsn = split_scoped_package_name('@angular')
    >>> assert ('@angular', None) == nsn, nsn
    >>> nsn = split_scoped_package_name('angular/')
    >>> assert (None, 'angular') == nsn, nsn
    >>> nsn = split_scoped_package_name('%2fangular%2f/ ')
    >>> assert (None, 'angular') == nsn, nsn
    """
    if not name:
        return None, None

    name = name and name.strip()
    if not name:
        return None, None

    name = name.replace('%40', '@').replace('%2f', '/').replace('%2F', '/')
    name = name.rstrip('@').strip('/').strip()
    if not name:
        return None, None

    # this should never happen: wee only have a scope.
    # TODO: raise an  exception?
    if is_scoped_package(name) and '/' not in name:
        return name, None

    ns, _, name = name.rpartition('/')
    ns = ns.strip() or None
    name = name.strip() or None
    return ns, name


def get_declared_licenses(license_object):
    """
    Return a list of declared licenses, either strings or dicts.
    """
    if not license_object:
        return []

    if isinstance(license_object, str):
        # current, up to date form
        return [license_object]

    declared_licenses = []
    if isinstance(license_object, dict):
        # old, deprecated forms
        """
         "license": {
            "type": "MIT",
            "url": "http://github.com/kriskowal/q/raw/master/LICENSE"
          }
        """
        declared_licenses.append(license_object)

    elif isinstance(license_object, list):
        # old, deprecated forms
        """
        "licenses": [{"type": "Apache License, Version 2.0",
                      "url": "http://www.apache.org/licenses/LICENSE-2.0" } ]
        or
        "licenses": ["MIT"],
        """
        declared_licenses.extend(license_object)
    return declared_licenses


def licenses_mapper(license, licenses, package):  # NOQA
    """
    Update package licensing and return package based on the `license` and
    `licenses` values found in a package.

    Licensing data structure has evolved over time and is a tad messy.
    https://docs.npmjs.com/files/package.json#license
    license(s) is either:
    - a string with:
     - an SPDX id or expression { "license" : "(ISC OR GPL-3.0)" }
     - some license name or id
     - "SEE LICENSE IN <filename>"
    - (Deprecated) an array or a list of arrays of type, url.
    -  "license": "UNLICENSED" means this is proprietary
    """
    declared_license = get_declared_licenses(license) or []
    declared_license.extend(get_declared_licenses(licenses)  or [])
    package.declared_license = declared_license
    return package


def party_mapper(party, package, party_type):
    """
    Update package parties with party of `party_type` and return package.
    https://docs.npmjs.com/files/package.json#people-fields-author-contributors
    """
    if isinstance(party, list):
        for auth in party:
            name, email, url = parse_person(auth)
            package.parties.append(models.Party(
                type=models.party_person,
                name=name,
                role=party_type,
                email=email,
                url=url))
    else:
        # a string or dict
        name, email, url = parse_person(party)
        package.parties.append(models.Party(
            type=models.party_person,
            name=name,
            role=party_type,
            email=email,
            url=url))

    return package


def bugs_mapper(bugs, package):
    """
    Update package bug tracker and support email and return package.
    https://docs.npmjs.com/files/package.json#bugs
    The url to your project's issue tracker and / or the email address to
    which issues should be reported.
    { "url" : "https://github.com/owner/project/issues"
    , "email" : "project@hostname.com"
    }
    You can specify either one or both values. If you want to provide only a
    url, you can specify the value for "bugs" as a simple string instead of an
    object.
    """
    if isinstance(bugs, str):
        package.bug_tracking_url = bugs
    elif isinstance(bugs, dict):
        # we ignore the bugs email for now
        package.bug_tracking_url = bugs.get('url')
    return package


def vcs_repository_mapper(repo, package, vcs_revision=None):
    """
    https://docs.npmjs.com/files/package.json#repository
    "repository" :
      { "type" : "git"
      , "url" : "https://github.com/npm/npm.git"
      }
    "repository" :
      { "type" : "svn"
      , "url" : "https://v8.googlecode.com/svn/trunk/"
      }
    """
    if not repo:
        return package

    if isinstance(repo, list):
        # There is a case where we can have a list with a single element
        repo = repo[0]

    vcs_tool = ''
    vcs_repository = ''

    if isinstance(repo, str):
        vcs_repository = normalize_vcs_url(repo)

    elif isinstance(repo, dict):
        repo_url = normalize_vcs_url(repo.get('url'))
        if repo_url:
            vcs_tool = repo.get('type') or 'git'
            # remove vcs_tool string if repo_url already contains it
            if repo_url.startswith(vcs_tool):
                vcs_tool = ''
            vcs_repository = repo_url

    if vcs_repository:
        if vcs_tool:
            vcs_url = '{}+{}'.format(vcs_tool, vcs_repository)
        else:
            vcs_url = vcs_repository

        if vcs_revision:
            vcs_url += '@' + vcs_revision
        package.vcs_url = vcs_url
    return package


def dist_mapper(dist, package):
    """
    Only present in some package.json forms (as installed or from a
    registry). Not documented.
    "dist": {
      "integrity: "sha512-VmqXvL6aSOb+rmswek7prvdFKsFbfMshcRRi07SdFyDqgG6uXsP276NkPTcrD0DiwVQ8rfnCUP8S90x0OD+2gQ==",
      "shasum": "a124386bce4a90506f28ad4b1d1a804a17baaf32",
      "dnl_url": "http://registry.npmjs.org/npm/-/npm-2.13.5.tgz"
      },
    """
    if not isinstance(dist, dict):
        return

    integrity = dist.get('integrity') or None
    if integrity:
        algo, _, b64value = integrity.partition('-')
        algo = algo.lower()
        assert 'sha512' == algo

        decoded_b64value = base64.b64decode(b64value)
        if isinstance(decoded_b64value, str):
            sha512 = decoded_b64value.encode('hex')
        elif isinstance(decoded_b64value, bytes):
            sha512 = decoded_b64value.hex()
        package.sha512 = sha512

    sha1 = dist.get('shasum')
    if sha1:
        package.sha1 = sha1

    dnl_url = dist.get('dnl_url')
    if not dnl_url:
        # Only add a synthetic download URL if there is none from the dist mapping.
        dnl_url = npm_download_url(package.namespace, package.name, package.version)
    package.download_url = dnl_url.strip()

    return package


def bundle_deps_mapper(bundle_deps, package):
    """
    https://docs.npmjs.com/files/package.json#bundleddependencies
        "This defines an array of package names that will be bundled
        when publishing the package."
    """
    for bdep in (bundle_deps or []):
        bdep = bdep and bdep.strip()
        if not bdep:
            continue

        ns, name = split_scoped_package_name(bdep)
        purl = models.PackageURL(type='npm', namespace=ns, name=name)

        dep = models.DependentPackage(purl=purl.to_string(),
            scope='bundledDependencies', is_runtime=True,
        )
        package.dependencies.append(dep)

    return package


def deps_mapper(deps, package, field_name):
    """
    Handle deps such as dependencies, devDependencies, peerDependencies, optionalDependencies
    return a tuple of (dep type, list of deps)
    https://docs.npmjs.com/files/package.json#dependencies
    https://docs.npmjs.com/files/package.json#peerdependencies
    https://docs.npmjs.com/files/package.json#devdependencies
    https://docs.npmjs.com/files/package.json#optionaldependencies
    """
    npm_dependency_scopes_attributes = {
        'dependencies': dict(is_runtime=True, is_optional=False),
        'devDependencies': dict(is_runtime=False, is_optional=True),
        'peerDependencies': dict(is_runtime=True, is_optional=False),
        'optionalDependencies': dict(is_runtime=True, is_optional=True),
    }
    dependencies = package.dependencies

    deps_by_name = {}
    if field_name == 'optionalDependencies':
        # optionalDependencies override the dependencies with the same name
        # so we build a map of name->dep object for use later
        for d in dependencies:
            if d.scope != 'dependencies':
                continue
            purl = PackageURL.from_string(d.purl)
            npm_name = purl.name
            if purl.namespace:
                npm_name = '/'.join([purl.namespace, purl.name])
            deps_by_name[npm_name] = d

    for fqname, requirement in deps.items():
        ns, name = split_scoped_package_name(fqname)
        if not name:
            continue
        purl = PackageURL(type='npm', namespace=ns, name=name).to_string()

        # optionalDependencies override the dependencies with the same name
        # https://docs.npmjs.com/files/package.json#optionaldependencies
        # therefore we update/override the dependency of the same name
        overridable = deps_by_name.get(fqname)

        if overridable and field_name == 'optionalDependencies':
            overridable.purl = purl
            overridable.is_optional = True
            overridable.scope = field_name
        else:
            dependency_attributes = npm_dependency_scopes_attributes.get(field_name, dict())
            dep = models.DependentPackage(
                purl=purl,
                scope=field_name,
                requirement=requirement,
                **dependency_attributes
            )
            dependencies.append(dep)

    return package


person_parser = re.compile(
    r'^(?P<name>[^\(<]+)'
    r'\s?'
    r'(?P<email><([^>]+)>)?'
    r'\s?'
    r'(?P<url>\([^\)]+\))?$'
).match

person_parser_no_name = re.compile(
    r'(?P<email><([^>]+)>)?'
    r'\s?'
    r'(?P<url>\([^\)]+\))?$'
).match


class NpmInvalidPerson(Exception):
    pass


def parse_person(person):
    """
    https://docs.npmjs.com/files/package.json#people-fields-author-contributors
    A "person" is an object with a "name" field and optionally "url" and "email".

    Return a name, email, url tuple for a person object
    A person can be in the form:
      "author": {
        "name": "Isaac Z. Schlueter",
        "email": "i@izs.me",
        "url": "http://blog.izs.me"
      },
    or in the form:
      "author": "Isaac Z. Schlueter <i@izs.me> (http://blog.izs.me)",

    Both forms are equivalent.

    For example:
    >>> author = {
    ...   "name": "Isaac Z. Schlueter",
    ...   "email": "i@izs.me",
    ...   "url": "http://blog.izs.me"
    ... }
    >>> p = parse_person(author)
    >>> assert p == (u'Isaac Z. Schlueter', u'i@izs.me', u'http://blog.izs.me')
    >>> p = parse_person('Barney Rubble <b@rubble.com> (http://barnyrubble.tumblr.com/)')
    >>> assert p == (u'Barney Rubble', u'b@rubble.com', u'http://barnyrubble.tumblr.com/')
    >>> p = parse_person('Barney Rubble <none> (none)')
    >>> assert p == (u'Barney Rubble', None, None)
    >>> p = parse_person('Barney Rubble ')
    >>> assert p == (u'Barney Rubble', None, None)
    >>> author = {
    ...   "name": "Isaac Z. Schlueter",
    ...   "email": ["i@izs.me", "<jo2@todo.com> "],
    ...   "url": "http://blog.izs.me"
    ... }
    >>> p = parse_person(author)
    >>> assert p == (u'Isaac Z. Schlueter', u'i@izs.me\\njo2@todo.com', u'http://blog.izs.me')
    >>> p = parse_person('<b@rubble.com> (http://barnyrubble.tumblr.com/)')
    >>> assert p == (None, u'b@rubble.com', u'http://barnyrubble.tumblr.com/')
    """
    # TODO: detect if this is a person name or a company name e.g. the type?

    name = None
    email = None
    url = None

    if isinstance(person, str):
        parsed = person_parser(person)
        if not parsed:
            parsed = person_parser_no_name(person)
            if not parsed:
                return person, None, None
            else:
                name = None
                email = parsed.group('email')
                url = parsed.group('url')
        else:
            name = parsed.group('name')
            email = parsed.group('email')
            url = parsed.group('url')

    elif isinstance(person, dict):
        # ensure we have our three values
        name = person.get('name')
        email = person.get('email')
        url = person.get('url')

    else:
        return None, None, None

    if name:
        if isinstance(name, str):
            name = name.strip()
            if name.lower() == 'none':
                name = None
        else:
            name = None
    name = name or None

    if email:
        if isinstance(email, list):
            # legacy weirdness
            email = [e.strip('<> ') for e in email if e and e.strip()]
            email = '\n'.join([e.strip() for e in email
                               if e.strip() and e.strip().lower() != 'none'])
        if isinstance(email, str):
            email = email.strip('<> ').strip()
            if email.lower() == 'none':
                email = None
        else:
            email = None
    email = email or None

    if url:
        if isinstance(url, list):
            # legacy weirdness
            url = [u.strip('() ') for u in email if u and u.strip()]
            url = '\n'.join([u.strip() for u in url
                               if u.strip() and u.strip().lower() != 'none'])
        if isinstance(url, str):
            url = url.strip('() ').strip()
            if url.lower() == 'none':
                url = None
        else:
            url = None
    url = url or None

    return name, email, url


def keywords_mapper(keywords, package):
    """
    Update package keywords and return package.
    This is supposed to be an array of strings, but sometimes this is a string.
    https://docs.npmjs.com/files/package.json#keywords
    """
    if isinstance(keywords, str):
        if ',' in keywords:
            keywords = [k.strip() for k in keywords.split(',') if k.strip()]
        else:
            keywords = [keywords]

    package.keywords = keywords
    return package


def build_packages_from_lockfile(package_data):
    """
    Yield `NpmPackage`s from a parsed package-lock.json or npm-shrinkwrap.json
    file
    """
    packages = []
    dev_packages = []
    for dependency, values in package_data.get('dependencies', {}).items():
        is_dev = values.get('dev', False)
        dep_deps = []
        namespace = None
        name = dependency
        if '/' in dependency:
            namespace, _slash, name = dependency.partition('/')
        # Handle the case where an entry in `dependencies` from a
        # package-lock.json file has `requires`
        for dep, dep_req in values.get('requires', {}).items():
            purl = PackageURL(type='npm', name=dep, version=dep_req).to_string()
            if is_dev:
                dep_deps.append(
                    models.DependentPackage(
                        purl=purl,
                        scope='requires-dev',
                        requirement=dep_req,
                        is_runtime=False,
                        is_optional=True,
                        is_resolved=True,
                    )
                )
            else:
                dep_deps.append(
                    models.DependentPackage(
                        purl=purl,
                        scope='requires',
                        requirement=dep_req,
                        is_runtime=True,
                        is_optional=False,
                        is_resolved=True,
                    )
                )

        # Handle the case where an entry in `dependencies` from a
        # npm-shrinkwrap.json file has `dependencies`
        for dep, dep_values in values.get('dependencies', {}).items():
            dep_version = dep_values.get('version')
            requirement = None
            split_requirement = dep_values.get('from', '').rsplit('@')
            if len(split_requirement) == 2:
                # requirement is in the form of "abbrev@>=1.0.0 <2.0.0", so we just want what is right of @
                _, requirement = split_requirement
            dep_deps.append(
                models.DependentPackage(
                    purl=PackageURL(
                        type='npm',
                        name=dep,
                        version=dep_version,
                    ).to_string(),
                    scope='dependencies',
                    requirement=requirement,
                    is_runtime=True,
                    is_optional=False,
                    is_resolved=True,
                )
            )

        p = NpmPackage(
            namespace=namespace,
            name=name,
            version=values.get('version'),
            download_url=values.get('resolved'),
            dependencies=dep_deps,
        )
        if is_dev:
            dev_packages.append(p)
        else:
            packages.append(p)

    package_deps = []
    for package in packages:
        package_deps.append(
            models.DependentPackage(
                purl=package.purl,
                scope='dependencies',
                is_runtime=True,
                is_optional=False,
                is_resolved=True,
            )
        )

    dev_package_deps = []
    for dev_package in dev_packages:
        dev_package_deps.append(
            models.DependentPackage(
                purl=dev_package.purl,
                scope='dependencies-dev',
                is_runtime=False,
                is_optional=True,
                is_resolved=True,
            )
        )

    yield NpmPackage(
        name=package_data.get('name'),
        version=package_data.get('version'),
        dependencies=package_deps + dev_package_deps,
    )

    for package in packages + dev_packages:
        yield package


def build_packages_from_yarn_lock(yarn_lock_lines):
    """
    Yield `NpmPackage`s from a list of lines of a yarn.lock file
    """
    packages = []
    packages_reqs = []
    current_package_data = {}
    dependencies = False
    for line in yarn_lock_lines:
        # Check if this is not an empty line or comment
        if line.strip() and not line.startswith('#'):
            if line.startswith(' '):
                if not dependencies:
                    if line.strip().startswith('version'):
                        current_package_data['version'] = line.partition('version')[2].strip().strip('\"')
                    elif line.strip().startswith('resolved'):
                        current_package_data['download_url'] = line.partition('resolved')[2].strip().strip('\"')
                    elif line.strip().startswith('dependencies'):
                        dependencies = True
                    else:
                        continue
                else:
                    split_line = line.strip().split()
                    if len(split_line) == 2:
                        k, v = split_line
                        # Remove leading and following quotation marks on values
                        v = v[1:-1]
                        if 'dependencies' in current_package_data:
                            current_package_data['dependencies'].append((k, v))
                        else:
                            current_package_data['dependencies'] = [(k, v)]
            else:
                dependencies = False
                # Clean up dependency name and requirement strings
                line = line.strip().replace('\"', '').replace(':', '')
                split_line_for_name_req = line.split(',')
                dep_names_and_reqs = defaultdict(list)
                for segment in split_line_for_name_req:
                    segment = segment.strip()
                    dep_name_and_req = segment.rsplit('@', 1)
                    dep, req = dep_name_and_req
                    dep_names_and_reqs[dep].append(req)
                # We should only have one key `dep_names_and_reqs`, where it is the
                # current dependency we are looking at
                if len(dep_names_and_reqs.keys()) == 1:
                    dep, req = list(dep_names_and_reqs.items())[0]
                    if '@' in dep and '/' in dep:
                        namespace, name = dep.split('/')
                        current_package_data['namespace'] = namespace
                        current_package_data['name'] = name
                    else:
                        current_package_data['name'] = dep
                    current_package_data['requirement'] = ', '.join(req)
        else:
            deps = []
            if current_package_data:
                for dep, req in current_package_data.get('dependencies', []):
                    deps.append(
                        models.DependentPackage(
                            purl=PackageURL(type='npm', name=dep).to_string(),
                            scope='dependencies',
                            requirement=req,
                            is_runtime=True,
                            is_optional=False,
                            is_resolved=True,
                        )
                    )
                packages.append(
                    NpmPackage(
                        namespace=current_package_data.get('namespace'),
                        name=current_package_data.get('name'),
                        version=current_package_data.get('version'),
                        download_url=current_package_data.get('download_url'),
                        dependencies=deps
                    )
                )
                packages_reqs.append(current_package_data.get('requirement'))
                current_package_data = {}

    # Add the last element if it's not already added
    if current_package_data:
        for dep, req in current_package_data.get('dependencies', []):
            deps.append(
                models.DependentPackage(
                    purl=PackageURL(type='npm', name=dep).to_string(),
                    scope='dependencies',
                    requirement=req,
                    is_runtime=True,
                    is_optional=False,
                    is_resolved=True,
                )
            )
        packages.append(
            NpmPackage(
                namespace=current_package_data.get('namespace'),
                name=current_package_data.get('name'),
                version=current_package_data.get('version'),
                download_url=current_package_data.get('download_url'),
                dependencies=deps
            )
        )
        packages_reqs.append(current_package_data.get('requirement'))

    packages_dependencies = []
    for package, requirement in zip(packages, packages_reqs):
        packages_dependencies.append(
            models.DependentPackage(
                purl=PackageURL(
                    type='npm',
                    name=package.name,
                    version=package.version
                ).to_string(),
                requirement=requirement,
                scope='dependencies',
                is_runtime=True,
                is_optional=False,
                is_resolved=True,
            )
        )

    yield NpmPackage(dependencies=packages_dependencies)
    for package in packages:
        yield package
