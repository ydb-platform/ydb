#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import sys

import attr
from packageurl import normalize_qualifiers
from packageurl import PackageURL

from commoncode.datautils import choices
from commoncode.datautils import Boolean
from commoncode.datautils import Date
from commoncode.datautils import Integer
from commoncode.datautils import List
from commoncode.datautils import Mapping
from commoncode.datautils import String
from commoncode.datautils import TriBoolean

"""
Data models for package information and dependencies, abstracting the
differences existing between package formats and tools.

A package has a somewhat fuzzy definition and is code that can be consumed and
provisioned by a package manager or can be installed.

It can be a single file such as script; more commonly a package is stored in an
archive or directory.

A package contains:
 - information typically in a "manifest" file,
 - a payload of code, doc, data.

Structured package information are found in multiple places:
- in manifest file proper (such as a Maven POM, NPM package.json and many others)
- in binaries (such as an Elf or LKM, Windows PE or RPM header).
- in code (JavaDoc tags or Python __copyright__ magic)
There are collectively named "manifests" in ScanCode.

We handle package information at two levels:
1.- package information collected in a "manifest" at a file level
2.- aggregated package information based on "manifest" at a directory or archive
level (or in some rarer cases file level)

The second requires the first to be computed.
The schema for these two is the same.
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


class BaseModel(object):
    """
    Base class for all package models.
    """

    def to_dict(self, **kwargs):
        """
        Return an dict of primitive Python types.
        """
        return attr.asdict(self)

    @classmethod
    def create(cls, **kwargs):
        """
        Return an object built from ``kwargs``. Always ignore unknown attributes
        provided in ``kwargs`` that do not exist as declared attr fields in
        ``cls``.
        """
        known_attr = cls.fields()
        kwargs = {k: v for k, v in kwargs.items() if k in known_attr}
        return cls(**kwargs)


    @classmethod
    def fields(cls):
        """
        Return a list of field names defined on this model.
        """
        return list(attr.fields_dict(cls))


party_person = 'person'
# often loosely defined
party_project = 'project'
# more formally defined
party_org = 'organization'
PARTY_TYPES = (
    None,
    party_person,
    party_project,
    party_org,
)


@attr.s()
class Party(BaseModel):
    """
    A party is a person, project or organization related to a package.
    """

    type = String(
        repr=True,
        validator=choices(PARTY_TYPES),
        label='party type',
        help='the type of this party: One of: '
            +', '.join(p for p in PARTY_TYPES if p))

    role = String(
        repr=True,
        label='party role',
        help='A role for this party. Something such as author, '
             'maintainer, contributor, owner, packager, distributor, '
             'vendor, developer, owner, etc.')

    name = String(
        repr=True,
        label='name',
        help='Name of this party.')

    email = String(
        repr=True,
        label='email',
        help='Email for this party.')

    url = String(
        repr=True,
        label='url',
        help='URL to a primary web page for this party.')


@attr.s()
class BasePackage(BaseModel):
    """
    A base identifiable package object using discrete identifying attributes as
    specified here https://github.com/package-url/purl-spec.
    """

    # class-level attributes used to recognize a package
    filetypes = tuple()
    mimetypes = tuple()
    extensions = tuple()
    # list of known metafiles for a package type
    metafiles = []

    # Optional. Public default web base URL for package homepages of this
    # package type on the default repository.
    default_web_baseurl = None

    # Optional. Public default download base URL for direct downloads of this
    # package type the default repository.
    default_download_baseurl = None

    # Optional. Public default API repository base URL for package API calls of
    # this package type on the default repository.
    default_api_baseurl = None

    # Optional. Public default type for a package class.
    default_type = None

    # TODO: add description of the Package type for info
    # type_description = None

    type = String(
        repr=True,
        label='package type',
        help='Optional. A short code to identify what is the type of this '
             'package. For instance gem for a Rubygem, docker for container, '
             'pypi for Python Wheel or Egg, maven for a Maven Jar, '
             'deb for a Debian package, etc.')

    namespace = String(
        repr=True,
        label='package namespace',
        help='Optional namespace for this package.')

    name = String(
        repr=True,
        label='package name',
        help='Name of the package.')

    version = String(
        repr=True,
        label='package version',
        help='Optional version of the package as a string.')

    qualifiers = Mapping(
        default=None,
        value_type=str,
        converter=lambda v: normalize_qualifiers(v, encode=False),
        label='package qualifiers',
        help='Optional mapping of key=value pairs qualifiers for this package')

    subpath = String(
        label='extra package subpath',
        help='Optional extra subpath inside a package and relative to the root '
             'of this package')

    def __attrs_post_init__(self, *args, **kwargs):
        if not self.type and hasattr(self, 'default_type'):
            self.type = self.default_type

    @property
    def purl(self):
        """
        Return a compact purl package URL string.
        """
        if not self.name:
            return
        return PackageURL(
            self.type, self.namespace, self.name, self.version,
            self.qualifiers, self.subpath).to_string()

    def repository_homepage_url(self, baseurl=default_web_baseurl):
        """
        Return the package repository homepage URL for this package, e.g. the
        URL to the page for this package in its package repository. This is
        typically different from the package homepage URL proper.
        Subclasses should override to provide a proper value.
        """
        return

    def repository_download_url(self, baseurl=default_download_baseurl):
        """
        Return the package repository download URL to download the actual
        archive of code of this package. This may be different than the actual
        download URL and is computed from the default public respoitory baseurl.
        Subclasses should override to provide a proper value.
        """
        return

    def api_data_url(self, baseurl=default_api_baseurl):
        """
        Return the package repository API URL to obtain structured data for this
        package such as the URL to a JSON or XML api.
        Subclasses should override to provide a proper value.
        """
        return

    def set_purl(self, package_url):
        """
        Update this Package object with the `package_url` purl string or
        PackageURL attributes.
        """
        if not package_url:
            return

        if not isinstance(package_url, PackageURL):
            package_url = PackageURL.from_string(package_url)

        attribs = ['type', 'namespace', 'name', 'version', 'qualifiers', 'subpath']
        for att in attribs:
            self_val = getattr(self, att)
            purl_val = getattr(package_url, att)
            if not self_val and purl_val:
                setattr(self, att, purl_val)

    def to_dict(self, **kwargs):
        """
        Return an dict of primitive Python types.
        """
        mapping = attr.asdict(self)
        mapping['purl'] = self.purl
        mapping['repository_homepage_url'] = self.repository_homepage_url()
        mapping['repository_download_url'] = self.repository_download_url()
        mapping['api_data_url'] = self.api_data_url()

        if self.qualifiers:
            mapping['qualifiers'] = normalize_qualifiers(
                qualifiers=self.qualifiers,
                encode=False,
            )
        return mapping

    @classmethod
    def create(cls, **kwargs):
        """
        Return a Package built from ``kwargs``. Always ignore unknown attributes
        provided in ``kwargs`` that do not exist as declared attr fields in
        ``cls``.
        """
        from packagedcode import get_package_class
        type_cls = get_package_class(kwargs, default=cls)
        return super(BasePackage, type_cls).create(**kwargs)


@attr.s()
class DependentPackage(BaseModel):
    """
    An identifiable dependent package package object.
    """

    purl = String(
        repr=True,
        label='Dependent package URL',
        help='A compact purl package URL. Typically when there is an '
             'unresolved requirement, there is no version. '
             'If the dependency is resolved, the version should be added to '
             'the purl')

    requirement = String(
        repr=True,
        label='dependent package version requirement',
        help='A string defining version(s)requirements. Package-type specific.')

    scope = String(
        repr=True,
        label='dependency scope',
        help='The scope of this dependency, such as runtime, install, etc. '
        'This is package-type specific and is the original scope string.')

    is_runtime = Boolean(
        default=True,
        label='is runtime flag',
        help='True if this dependency is a runtime dependency.')

    is_optional = Boolean(
        default=False,
        label='is optional flag',
        help='True if this dependency is an optional dependency')

    is_resolved = Boolean(
        default=False,
        label='is resolved flag',
        help='True if this dependency version requirement has '
             'been resolved and this dependency url points to an '
             'exact version.')


@attr.s()
class PackageFile(BaseModel):
    """
    A file that belongs to a package.
    """

    path = String(
        label='Path of this installed file',
        help='The path of this installed file either relative to a rootfs '
             '(typical for system packages) or a path in this scan (typical '
             'for application packages).',
        repr=True,
    )

    size = Integer(
        label='file size',
        help='size of the file in bytes')

    sha1 = String(
        label='SHA1 checksum',
        help='SHA1 checksum for this file in hexadecimal')

    md5 = String(
        label='MD5 checksum',
        help='MD5 checksum for this file in hexadecimal')

    sha256 = String(
        label='SHA256 checksum',
        help='SHA256 checksum for this file in hexadecimal')

    sha512 = String(
        label='SHA512 checksum',
        help='SHA512 checksum for this file in hexadecimal')


@attr.s()
class Package(BasePackage):
    """
    A package object as represented by its manifest data.
    """

    # Optional. Public default type for a package class.
    default_primary_language = None

    primary_language = String(
        label='Primary programming language',
        help='Primary programming language',)

    description = String(
        label='Description',
        help='Description for this package. '
             'By convention the first should be a summary when available.')

    release_date = Date(
        label='release date',
        help='Release date of the package')

    parties = List(
        item_type=Party,
        label='parties',
        help='A list of parties such as a person, project or organization.')

    keywords = List(
        item_type=str,
        label='keywords',
        help='A list of keywords.')

    homepage_url = String(
        label='homepage URL',
        help='URL to the homepage for this package.')

    download_url = String(
        label='Download URL',
        help='A direct download URL.')

    size = Integer(
        default=None,
        label='download size',
        help='size of the package download in bytes')

    sha1 = String(
        label='SHA1 checksum',
        help='SHA1 checksum for this download in hexadecimal')

    md5 = String(
        label='MD5 checksum',
        help='MD5 checksum for this download in hexadecimal')

    sha256 = String(
        label='SHA256 checksum',
        help='SHA256 checksum for this download in hexadecimal')

    sha512 = String(
        label='SHA512 checksum',
        help='SHA512 checksum for this download in hexadecimal')

    bug_tracking_url = String(
        label='bug tracking URL',
        help='URL to the issue or bug tracker for this package')

    code_view_url = String(
        label='code view URL',
        help='a URL where the code can be browsed online')

    vcs_url = String(
        help='a URL to the VCS repository in the SPDX form of: '
             'https://github.com/nexb/scancode-toolkit.git@405aaa4b3 '
              'See SPDX specification "Package Download Location" '
              'at https://spdx.org/spdx-specification-21-web-version#h.49x2ik5 ')

    copyright = String(
        label='Copyright',
        help='Copyright statements for this package. Typically one per line.')

    license_expression = String(
        label='license expression',
        help='The license expression for this package typically derived '
             'from its declared license or from some other type-specific '
             'routine or convention.')

    declared_license = String(
        label='declared license',
        help='The declared license mention, tag or text as found in a '
             'package manifest. This can be a string, a list or dict of '
             'strings possibly nested, as found originally in the manifest.')

    notice_text = String(
        label='notice text',
        help='A notice text for this package.')

    root_path = String(
        label='package root path',
        help='The path to the root of the package documented in this manifest '
             'if any, such as a Maven .pom or a npm package.json parent directory.')

    dependencies = List(
        item_type=DependentPackage,
        label='dependencies',
        help='A list of DependentPackage for this package. ')

    contains_source_code = TriBoolean(
        label='contains source code',
        help='Flag set to True if this package contains its own source code, None '
             'if this is unknown, False if not.')

    source_packages = List(
        item_type=String,
        label='List of related source code packages',
        help='A list of related  source code Package URLs (aka. "purl") for '
             'this package. For instance an SRPM is the "source package" for a '
             'binary RPM.')

    installed_files = List(
        item_type=PackageFile,
        label='installed files',
        help='List of files installed by this package.')

    extra_data = Mapping(
        label='extra data',
        help='A mapping of arbitrary extra Package data.')

    def __attrs_post_init__(self, *args, **kwargs):
        if not self.type and hasattr(self, 'default_type'):
            self.type = self.default_type

        if not self.primary_language and hasattr(self, 'default_primary_language'):
            self.primary_language = self.default_primary_language

    @classmethod
    def recognize(cls, location):
        """
        Yield one or more Package objects given a file location pointing to a
        package archive, manifest or similar.

        Sub-classes should override to implement their own package recognition.
        """
        raise NotImplementedError

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        """
        Return the Resource for the package root given a `manifest_resource`
        Resource object that represents a manifest in the `codebase` Codebase.

        Each package type and instance have different conventions on how a
        package manifest relates to the root of a package.

        For instance, given a "package.json" file, the root of an npm is the
        parent directory. The same applies with a Maven "pom.xml". In the case
        of a "xyz.pom" file found inside a JAR META-INF/ directory, the root is
        the JAR itself which may not be the direct parent

        Each package type should subclass as needed. This default to return the
        same path.
        """
        return manifest_resource

    @classmethod
    def get_package_resources(cls, package_root, codebase):
        """
        Yield the Resources of a Package starting from `package_root`
        """
        if not Package.is_ignored_package_resource(package_root, codebase):
            yield package_root
        for resource in package_root.walk(codebase, topdown=True, ignored=Package.is_ignored_package_resource):
            yield resource

    @classmethod
    def ignore_resource(cls, resource, codebase):
        """
        Return True if `resource` should be ignored.
        """
        return False

    @staticmethod
    def is_ignored_package_resource(resource, codebase):
        from packagedcode import PACKAGE_TYPES
        return any(pt.ignore_resource(resource, codebase) for pt in PACKAGE_TYPES)

    def compute_normalized_license(self):
        """
        Return a normalized license_expression string using the declared_license
        field. Return 'unknown' if there is a declared license but it cannot be
        detected and return None if there is no declared license

        Subclasses can override to handle specifics such as supporting specific
        license ids and conventions.
        """
        return compute_normalized_license(self.declared_license)

    @classmethod
    def extra_key_files(cls):
        """
        Return a list of extra key file paths (or path glob patterns) beyond
        standard, well known key files for this Package. List items are strings
        that are either paths or glob patterns and are relative to the package
        root.

        Knowing if a file is a "key-file" file is important for classification
        and summarization. For instance, a JAR can have key files that are not
        top level under the META-INF directory. Or a .gem archive contains a
        metadata.gz file.

        Sub-classes can implement as needed.
        """
        return []

    @classmethod
    def extra_root_dirs(cls):
        """
        Return a list of extra package root-like directory paths (or path glob
        patterns) that should be considered to determine if a files is a top
        level file or not. List items are strings that are either paths or glob
        patterns and are relative to the package root.

        Knowing if a file is a "top-level" file is important for classification
        and summarization.

        Sub-classes can implement as needed.
        """
        return []

    def to_dict(self, _detailed=False, **kwargs):
        data = super().to_dict(**kwargs)
        if _detailed:
            data['installed_files'] = [
                istf.to_dict() for istf in (self.installed_files or [])
            ]
        else:
            data.pop('installed_files', None)
        return data


def compute_normalized_license(declared_license):
    """
    Return a normalized license_expression string from the ``declared_license``.
    Return 'unknown' if there is a declared license but it cannot be detected
    (including on errors) and return None if there is no declared license.
    """

    if not declared_license:
        return

    from packagedcode import licensing
    try:
        return licensing.get_normalized_expression(declared_license)
    except Exception:
        # FIXME: add logging
        # we never fail just for this
        return 'unknown'

# Package types
# NOTE: this is somewhat redundant with extractcode archive handlers
# yet the purpose and semantics are rather different here


@attr.s()
class JavaJar(Package):
    metafiles = ('META-INF/MANIFEST.MF',)
    extensions = ('.jar',)
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip',)
    default_type = 'jar'
    default_primary_language = 'Java'


@attr.s()
class JavaWar(Package):
    metafiles = ('WEB-INF/web.xml',)
    extensions = ('.war',)
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip')
    default_type = 'war'
    default_primary_language = 'Java'


@attr.s()
class JavaEar(Package):
    metafiles = ('META-INF/application.xml', 'META-INF/ejb-jar.xml')
    extensions = ('.ear',)
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip')
    default_type = 'ear'
    default_primary_language = 'Java'


@attr.s()
class Axis2Mar(Package):
    """Apache Axis2 module"""
    metafiles = ('META-INF/module.xml',)
    extensions = ('.mar',)
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip')
    default_type = 'axis2'
    default_primary_language = 'Java'


@attr.s()
class JBossSar(Package):
    metafiles = ('META-INF/jboss-service.xml',)
    extensions = ('.sar',)
    filetypes = ('java archive ', 'zip archive',)
    mimetypes = ('application/java-archive', 'application/zip')
    default_type = 'jboss'
    default_primary_language = 'Java'


@attr.s()
class IvyJar(JavaJar):
    metafiles = ('ivy.xml',)
    default_type = 'ivy'
    default_primary_language = 'Java'


# FIXME: move to bower.py
@attr.s()
class BowerPackage(Package):
    metafiles = ('bower.json',)
    default_type = 'bower'
    default_primary_language = 'JavaScript'

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)


@attr.s()
class MeteorPackage(Package):
    metafiles = ('package.js',)
    default_type = 'meteor'
    default_primary_language = 'JavaScript'

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)


@attr.s()
class CpanModule(Package):
    metafiles = (
        '*.pod',
        '*.pm',
        'MANIFEST',
        'Makefile.PL',
        'META.yml',
        'META.json',
        '*.meta',
        'dist.ini',)
    # TODO: refine me
    extensions = ('.tar.gz',)
    default_type = 'cpan'
    default_primary_language = 'Perl'


# TODO: refine me: Go packages are a mess but something is emerging
# TODO: move to and use godeps.py
@attr.s()
class Godep(Package):
    metafiles = ('Godeps',)
    default_type = 'golang'
    default_primary_language = 'Go'

    @classmethod
    def get_package_root(cls, manifest_resource, codebase):
        return manifest_resource.parent(codebase)


@attr.s()
class AndroidApp(Package):
    filetypes = ('zip archive',)
    mimetypes = ('application/zip',)
    extensions = ('.apk',)
    default_type = 'android'
    default_primary_language = 'Java'


# see http://tools.android.com/tech-docs/new-build-system/aar-formats
@attr.s()
class AndroidLibrary(Package):
    filetypes = ('zip archive',)
    mimetypes = ('application/zip',)
    # note: Apache Axis also uses AAR extensions for plain Jars.
    # this could be decided based on internal structure
    extensions = ('.aar',)
    default_type = 'android-lib'
    default_primary_language = 'Java'


@attr.s()
class MozillaExtension(Package):
    filetypes = ('zip archive',)
    mimetypes = ('application/zip',)
    extensions = ('.xpi',)
    default_type = 'mozilla'
    default_primary_language = 'JavaScript'


@attr.s()
class ChromeExtension(Package):
    filetypes = ('data',)
    mimetypes = ('application/octet-stream',)
    extensions = ('.crx',)
    default_type = 'chrome'
    default_primary_language = 'JavaScript'


@attr.s()
class IOSApp(Package):
    filetypes = ('zip archive',)
    mimetypes = ('application/zip',)
    extensions = ('.ipa',)
    default_type = 'ios'
    default_primary_language = 'Objective-C'


@attr.s()
class CabPackage(Package):
    filetypes = ('microsoft cabinet',)
    mimetypes = ('application/vnd.ms-cab-compressed',)
    extensions = ('.cab',)
    default_type = 'cab'


@attr.s()
class MsiInstallerPackage(Package):
    filetypes = ('msi installer',)
    mimetypes = ('application/x-msi',)
    extensions = ('.msi',)
    default_type = 'msi'


@attr.s()
class InstallShieldPackage(Package):
    filetypes = ('installshield',)
    mimetypes = ('application/x-dosexec',)
    extensions = ('.exe',)
    default_type = 'installshield'


@attr.s()
class NSISInstallerPackage(Package):
    filetypes = ('nullsoft installer',)
    mimetypes = ('application/x-dosexec',)
    extensions = ('.exe',)
    default_type = 'nsis'


@attr.s()
class SharPackage(Package):
    filetypes = ('posix shell script',)
    mimetypes = ('text/x-shellscript',)
    extensions = ('.sha', '.shar', '.bin',)
    default_type = 'shar'


@attr.s()
class AppleDmgPackage(Package):
    filetypes = ('zlib compressed',)
    mimetypes = ('application/zlib',)
    extensions = ('.dmg', '.sparseimage',)
    default_type = 'dmg'


@attr.s()
class IsoImagePackage(Package):
    filetypes = ('iso 9660 cd-rom', 'high sierra cd-rom',)
    mimetypes = ('application/x-iso9660-image',)
    extensions = ('.iso', '.udf', '.img',)
    default_type = 'iso'


@attr.s()
class SquashfsPackage(Package):
    filetypes = ('squashfs',)
    default_type = 'squashfs'

# TODO: Add VM images formats(VMDK, OVA, OVF, VDI, etc) and Docker/other containers
