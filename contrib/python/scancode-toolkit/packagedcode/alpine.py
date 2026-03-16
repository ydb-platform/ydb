#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#
import base64
import codecs
from datetime import datetime
import email
import io
from os import path
import posixpath
import re

import attr
from license_expression import Licensing
from packageurl import PackageURL

from commoncode.datautils import List
from packagedcode import models
from packagedcode import licensing
from textcode.analysis import as_unicode


@attr.s()
class AlpinePackage(models.Package):
    extensions = ('.apk',)
    default_type = 'alpine'
    def compute_normalized_license(self):
        _declared, detected = detect_declared_license(self.declared_license)
        return detected

    def to_dict(self, _detailed=False, **kwargs):
        data = models.Package.to_dict(self, **kwargs)
        if _detailed:
            #################################################
            data['installed_files'] = [istf.to_dict() for istf in (self.installed_files or [])]
            #################################################
        else:
            #################################################
            # remove temporary fields
            data.pop('installed_files', None)
            #################################################

        return data


def get_installed_packages(root_dir, **kwargs):
    """
    Yield Package objects given a ``root_dir`` rootfs directory.
    """
    installed_file_loc = path.join(root_dir, 'lib/apk/db/installed')
    if not path.exists(installed_file_loc):
        return
    for package in parse_alpine_installed_db(installed_file_loc):
        yield package


def parse_alpine_installed_db(location):
    """
    Yield AlpinePackage objects from an installed database file at `location`
    or None. Typically found at '/lib/apk/db/installed' in an Alpine
    installation.

    Note: http://uk.alpinelinux.org/alpine/v3.11/main/x86_64/APKINDEX.tar.gz are
    also in the same format as an installed database.
    """
    if not path.exists(location):
        return

    with open(location, 'rb') as f:
        installed = as_unicode(f.read())

    if installed:
        # each paragrpah is seprated by LFLF
        packages = (p for p in re.split('\n\n', installed) if p)
        for pkg in packages:
            try:
                fields = email.message_from_string(pkg)
            except UnicodeEncodeError:
                fields = email.message_from_string(pkg.encode('utf-8'))
            fields = [(n.strip(), v.strip(),) for n, v in fields.items()]
            yield build_package(fields)


def build_package(package_fields):
    """
    Return an AlpinePackage object from a `package_fields` list of (name, value)
    tuples. The package_fields comes from the APKINDEX and installed database
    files that use one-letter field names.

    See for details:
    https://wiki.alpinelinux.org/wiki/Apk_spec#Install_DB
    https://wiki.alpinelinux.org/wiki/Alpine_package_format
    https://git.alpinelinux.org/apk-tools/tree/src/package.c?id=82de29cf7bad3d9cbb0aeb4dbe756ad2bde73eb3#n774
    """
    # mpping of actual Package field name -> value that have been converted to the expected format
    converted_fields = {}
    for name, value in package_fields:
        handler = package_short_field_handlers.get(name)
        if handler:
            converted = handler(value, **converted_fields)
            converted_fields.update(converted)

    # construct the package: we ignore unknown as we added a few technical fields
    package = AlpinePackage.create(**converted_fields)
    return package

# Note handlers MUST accept **kwargs as they also receive the current data
# being processed so far as a processed_data kwarg, but most do not use it


def name_value_str_handler(name):
    """
    Return a generic handler for plain string fields.
    """

    def handler(value, **kwargs):
        return {name: value}

    # name = name
    return handler


def L_license_handler(value, **kwargs):
    """
    Return a normalized declared license and a detected license expression.
    """
    declared, detected = detect_declared_license(value)
    return {
        'declared_license': declared,
        'license_expression': detected,
    }


def S_size_handler(value, **kwargs):
    return {'size': int(value)}


def t_release_date_handler(value, **kwargs):
    """
    Return a Package data mapping for a buiddate timestamp.
    """
    value = int(value)
    dt = datetime.utcfromtimestamp(value)
    stamp = dt.isoformat()
    # we get 2020-01-15T10:36:22, but care only for the date part
    date, _, _time = stamp.partition('T')
    return {'release_date': date}


get_maintainers = re.compile(
    r'(?P<name>[^<]+)'
    r'\s?'
    r'(?P<email><[^>]+>)'
).findall


def m_maintainer_handler(value, **kwargs):
    """
    Return a Package data mapping as a list of parties a maintainer Party.
    A maintainer value may be one or more mail name <email@example.com> parts, space-separated.
    """
    parties = []
    for name, email in get_maintainers(value):
        maintainer = models.Party(
            type='person',
            role='maintainer',
            name=name,
            email=email,
        )
        parties.append(maintainer)
    return {'parties': parties}


# this will return a three tuple if there is a split or a single item otherwise
split_name_and_requirement = re.compile('(=~|>~|<~|~=|~>|~<|=>|>=|=<|<=|<|>|=)').split


def D_dependencies_handler(value, dependencies=None, **kwargs):
    """
    Return a list of dependent packages from a dependency string and from previous dependencies.
    Dependencies can be either:
    - a package name with or without a version constraint
    - a path to something that is provided by some package(similar to RPMs) such as /bin/sh
    - a shared object (prefixed with so:)
    - a pkgconfig dependency where pkgconfig is typically also part of the deps
      and these are prefixed with pc:
    - a command prefixed with cmd:

    Note that these exist the same way in the p: provides field.

    An exclamation prefix negates the dependency.
    The operators can be ><=~

    For example:
        D:aaudit a52dec=0.7.4-r7 acf-alpine-baselayout>=0.5.7 /bin/sh
        D:so:libc.musl-x86_64.so.1 so:libmilter.so.1.0.2
        D:freeradius>3
        D:lua5.1-bit32<26
        D:!bison so:libc.musl-x86_64.so.1
        D:python3 pc:atk>=2.15.1 pc:cairo pc:xkbcommon>=0.2.0 pkgconfig
        D:bash colordiff cmd:ssh cmd:ssh-keygen cmd:ssh-keyscan cmd:column
        D:cmd:nginx nginx-mod-devel-kit so:libc.musl-aarch64.so.1
        D:mu=1.4.12-r0 cmd:emacs
    """
    # operate on a copy for safety and create an empty list on first use
    dependencies = dependencies[:] if dependencies else []
    for dep in value.split():
        if dep.startswith('!'):
            # ignore the "negative" deps, we cannot do much with it
            # they are more of a hints for the dep solver than something
            # actionable for origin reporting
            continue
        if dep.startswith('/'):
            # ignore paths to a command for now as we cannot do
            # much with them yet until we can have a Package URL for them.
            continue
        if dep.startswith('cmd:'):
            # ignore commands for now as we cannot do much with them yet until
            # we can have a Package URL for them.
            continue
        if dep.startswith('so:'):
            # ignore the shared object with an so: prexi for now as we cannot do
            # much with them yet until we can have a Package URL for them.
            # TODO: see how we could handle these and similar used elsewhere
            continue
        is_pc = False
        if dep.startswith('pc:'):
            is_pc = True
            # we strip the 'pc:' prefix and treat a pc: dependency the same as
            # other depends
            dep = dep[3:]

        requirement = None
        version = None
        is_resolved = False
        segments = split_name_and_requirement(dep)
        if len(segments) == 1:
            # we have no requirement...just a plain name
            name = dep
        else:
            if len(segments) != 3:
                raise Exception(dependencies, kwargs)
            name, operator, ver = segments
            # normalize operator tsuch that >= and => become =>
            operator = ''.join(sorted(operator))
            if operator == '=':
                version = ver
                is_resolved = True

            requirement = operator + ver

        purl = PackageURL(type='alpine', name=name, version=version).to_string()

        # that the only scope we have for now
        scope = 'depend'
        if is_pc:
            scope += ':pkgconfig'

        dependency = models.DependentPackage(
            purl=purl,
            scope=scope,
            requirement=requirement,
            is_resolved=is_resolved,
        )
        if dependency not in dependencies:
            dependencies.append(dependency)

    return {'dependencies': dependencies}


def o_source_package_handler(value, version=None, **kwargs):
    """
    Return a source_packages list of Package URLs
    """
    # the version value will be that of the current package
    origin = PackageURL(type='alpine', name=value, version=version).to_string()
    return {'source_packages': [origin]}


def c_git_commit_handler(value, **kwargs):
    """
    Return a git VCS URL from a package commit.
    """
    return {'vcs_url': 'git+http://git.alpinelinux.org/aports/commit/?id={}'.format(value)}


def A_arch_handler(value, **kwargs):
    """
    Return a Package URL qualifier for the arch.
    """
    return {'qualifiers': 'arch={}'.format(value)}

# Note that we use a little trick for handling files.
# Each handler receives a copy of the data processed so far.
# As it happens, the data about files start with a directory enry
# then one or more files, each normally followed by their checksums
# We return and use the current_dir and current_file from these handlers
# to properly create a file for its directory (which is the current one)
# and add the checksum to its file (which is the current one).
# 'current_file' and 'current_dir' are not actual package fields, but we ignore
# these when we create the AlpinePcakge object


def F_directory_handler(value, **kwargs):
    return {'current_dir': value}


def R_filename_handler(value, current_dir, installed_files=None, **kwargs):
    """
    Return a new current_file PackageFile in current_dir.  Add to installed_files
    """
    # operate on a copy for safety and create an empty list on first use
    installed_files = installed_files[:] if installed_files else []

    current_file = models.PackageFile(path=posixpath.join(current_dir, value))
    installed_files.append(current_file)
    return {'current_file': current_file, 'installed_files': installed_files}


def Z_checksum_handler(value, current_file, **kwargs):
    """
    Update the current PackageFile with its updated SHA1 hex-encoded checksum.

    'Z' is a file checksum (for files and links)
    For example: Z:Q1WTc55xfvPogzA0YUV24D0Ym+MKE=

    The 1st char is an encoding code: Q means base64 and the 2nd char is
    the type of checksum: 1 means SHA1 so Q1 means based64-encoded SHA1
    """
    assert value.startswith('Q1'), (
        'Unknown checksum or encoding: should start with Q1 for base64-encoded SHA1: {}'.format(value))
    sha1 = base64.b64decode(value[2:])
    sha1 = codecs.encode(sha1, 'hex').decode('utf-8').lower()
    current_file.sha1 = sha1
    return {'current_file': current_file}


# mapping of:
# - the package field one letter name in the installed db,
# - an handler for that field
package_short_field_handlers = {

    ############################################################################
    # per-package fields
    ############################################################################

    # name of the package
    # For example: P:busybox
    # 'pkgname' in .PKGINFO and APKBUILD
    'P': name_value_str_handler('name'),
    # this can be composed of two or three segments: epoch:version-release
    # For example: V:1.31.1-r9
    # 'pkver' in .PKGINFO and APKBUILD
    'V': name_value_str_handler('version'),
    # For example: T:Size optimized toolbox of many common UNIX utilities
    # 'pkgdesc' in .PKGINFO and APKBUILD
    'T': name_value_str_handler('description'),
    # For example: U:https://busybox.net/
    # 'url' in .PKGINFO and APKBUILD
    'U': name_value_str_handler('homepage_url'),
    # The license. For example: L:GPL2
    # 'license' in .PKGINFO and APKBUILD
    'L': L_license_handler,

    # For example: m:Natanael Copa <ncopa@alpinelinux.org>
    # 'maintainer' in .PKGINFO and APKBUILD
    'm': m_maintainer_handler,

    # For example: A:x86_64
    # 'arch' in .PKGINFO and APKBUILD
    'A':  A_arch_handler,

    # Compressed package size in bytes.
    # For example: S:507134
    # 'size' in .PKGINFO and APKBUILD
    'S': S_size_handler,

    # a build timestamp as in second since epoch
    # For example: t:1573846491
    # 'builddate' in .PKGINFO and APKBUILD
    't': t_release_date_handler,

    # origin and build_time are not set for virtual packages so we can skip these
    # name of the source package
    # For example: o:apk-tools
    # 'origin' in .PKGINFO and APKBUILD
    'o':  o_source_package_handler,

    # c is the sha1 "id" for the git commit in
    # https://git.alpinelinux.org/aports
    # e.g.: https://git.alpinelinux.org/aports/commit/?id=e5c984f68aabb28de623a7e3ada5a223c2b66d77
    # the commit is for the source package
    # For example: c:72048e01f0eef23b1300eb4c7d8eb2afb601f156
    # 'commit' in .PKGINFO and APKBUILD
    'c': c_git_commit_handler,

    # For example: D:scanelf so:libc.musl-x86_64.so.1
    # For example: D:so:libc.musl-x86_64.so.1 so:libcrypto.so.1.1 so:libssl.so.1.1 so:libz.so.1
    # Can occur more than once
    # 'depend' in .PKGINFO and APKBUILD
    'D': D_dependencies_handler,

    ############################################################################
    # ignored per-package fields. from here on, these fields are not used yet
    ############################################################################
    # For example: p:cmd:getconf cmd:getent cmd:iconv cmd:ldconfig cmd:ldd
    # ('p', 'provides'),

    # The as-installed size in bytes. For example: I:962560
    # ('I', 'installed_size'),

    # Checksum
    # For example: C:Q1sVrQyQ5Ek9/clI1rkKjgINqJNu8=
    # like for the file checksums "Z", Q means base64 encoding adn 1 means SHA1
    # The content used for this SHA1 is TBD.
    # ('C', 'checksum'),

    # not sure what this is for: TBD
    # ('i', 'install_if'),
    # 'r' if for dependencies replaces. For example: r:libiconv
    # For example: r:libiconv
    # ('r', 'replaces'),
    # not sure what this is for: TBD
    # ('k', 'provider_priority'),

    # 'q': Replaces Priority
    # 's': Get Tag Id?  or broken_script
    # 'f': broken_files?
    # 'x': broken_xattr?
    ############################################################################

    ############################################################################
    # per-file fields
    ############################################################################

    # - 'F': this is a folder path from the root and until there is a new F value
    #    all files defined with an R are under that folder
    'F': F_directory_handler,
    # - 'Z' is a file checksum (for files and links)
    'Z': Z_checksum_handler,
    # - 'R': this is a file name that is under the current F Folder
    'R': R_filename_handler,
    # - 'M' is a set of permissions for a folder as user:group:mode e.g. M:0:0:1777
    # - 'a' is a set of permissions for a file as user:group:mode e.g. a:0:0:755
    ############################################################################
}

############################################################################
# FIXME: this license detection code is copied from debian_copyright.py
############################################################################


def detect_declared_license(declared):
    """
    Return a tuple of (declared license, detected license expression) from a
    declared license. Both can be None.
    """
    declared = normalize_and_cleanup_declared_license(declared)
    if not declared:
        return None, None

    # apply multiple license detection in sequence
    detected = detect_using_name_mapping(declared)
    if detected:
        return declared, detected

    # cases of using a comma are for an AND
    normalized_declared = declared.replace(',', ' and ')
    detected = models.compute_normalized_license(normalized_declared)
    return declared, detected


def normalize_and_cleanup_declared_license(declared):
    """
    Return a cleaned and normalized declared license.
    """
    declared = declared or ''
    # there are few odd cases of license fileds starting with a colon or #
    declared = declared.strip(': \t#')
    # normalize spaces
    declared = ' '.join(declared.split())
    return declared


def detect_using_name_mapping(declared):
    """
    Return a license expression detected from a `declared` license string.
    """
    declared = declared.lower()
    detected = get_declared_to_detected().get(declared)
    if detected:
        licensing = Licensing()
        return str(licensing.parse(detected, simple=True))


_DECLARED_TO_DETECTED = None


def get_declared_to_detected(data_file=None):
    """
    Return a mapping of declared to detected license expression cached and
    loaded from a tab-separated text file, all lowercase, normalized for spaces.

    This data file is from license keys used in APKINDEX files and has been
    derived from a large collection of most APKINDEX files released by Alpine
    since circa Alpine 2.5.
    """
    global _DECLARED_TO_DETECTED
    if _DECLARED_TO_DETECTED:
        return _DECLARED_TO_DETECTED

    _DECLARED_TO_DETECTED = {}
    if not data_file:
        data_file = path.join(path.dirname(__file__), 'alpine_licenses.txt')
    with io.open(data_file, encoding='utf-8') as df:
        for line in df:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            decl, _, detect = line.partition('\t')
            if detect and detect.strip():
                decl = decl.strip()
                _DECLARED_TO_DETECTED[decl] = detect
    return _DECLARED_TO_DETECTED
