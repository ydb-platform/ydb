#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from collections import namedtuple
import logging
import os
import sys

import attr
from license_expression import Licensing

from packagedcode import models
from packagedcode import nevra
from packagedcode.pyrpm import RPM
from packagedcode.utils import build_description
from packagedcode import rpm_installed
import typecode.contenttype

TRACE = False


def logger_debug(*args):
    pass


logger = logging.getLogger(__name__)

if TRACE:
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))

# TODO: retrieve dependencies

# TODO: parse spec files see:
#  http://www.faqs.org/docs/artu/ch05s02.html#id2906931%29.)
#  http://code.activestate.com/recipes/436229-record-jar-parser/

RPM_TAGS = (
    'name',
    'epoch',
    'version',
    'release',
    'arch',
    'os',
    'summary',
    # the full description is often a long text
    'description',
    'distribution',
    'vendor',
    'license',
    'packager',
    'group',
    'url',
    'source_rpm',
    'dist_url',
    'is_binary',
)

RPMtags = namedtuple('RPMtags', list(RPM_TAGS))


def get_rpm_tags(location, include_desc=False):
    """
    Return an RPMtags object for the file at location or None.
    Include the long RPM description value if `include_desc` is True.
    """
    T = typecode.contenttype.get_type(location)

    if 'rpm' not in T.filetype_file.lower():
        return

    with open(location, 'rb') as rpmf:
        rpm = RPM(rpmf)
        tags = {k: v for k, v in rpm.to_dict().items() if k in RPM_TAGS}
        if not include_desc:
            tags['description'] = None
        return RPMtags(**tags)


class EVR(namedtuple('EVR', 'epoch version release')):
    """
    The RPM Epoch, Version, Release tuple.
    """

    # note: the order of the named tuple is the sort order.
    # But for creation we put the rarely used epoch last
    def __new__(self, version, release=None, epoch=None):
        if epoch and epoch.strip() and not epoch.isdigit():
            raise ValueError('Invalid epoch: must be a number or empty.')
        if not version:
            raise ValueError('Version is required: {}'.format(repr(version)))

        return super(EVR, self).__new__(EVR, epoch, version, release)

    def __str__(self, *args, **kwargs):
        return self.to_string()

    def to_string(self):
        if self.release:
            vr = '-'.join([self.version, self.release])
        else:
            vr = self.version

        if self.epoch:
            vr = ':'.join([self.epoch, vr])
        return vr


@attr.s()
class RpmPackage(models.Package):
    metafiles = ('*.spec',)
    extensions = ('.rpm', '.srpm', '.mvl', '.vip',)
    filetypes = ('rpm ',)
    mimetypes = ('application/x-rpm',)

    default_type = 'rpm'

    default_web_baseurl = None
    default_download_baseurl = None
    default_api_baseurl = None

    @classmethod
    def recognize(cls, location):
        yield parse(location)

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


def get_installed_packages(root_dir, detect_licenses=False, **kwargs):
    """
    Yield Package objects given a ``root_dir`` rootfs directory.
    """

    # TODO:  license and docs are typically at usr/share/doc/packages/<package name>/* and should be used
    # packages_doc_dir = os.path.join(root_dir, 'usr/share/doc/packages')
    # note that we also have file flags that can tell us which file is a license and doc.

    # dump the rpmdb to XMLish
    xmlish_loc = rpm_installed.collect_installed_rpmdb_xmlish_from_rootfs(root_dir)
    return rpm_installed.parse_rpm_xmlish(xmlish_loc, detect_licenses=detect_licenses)


def parse(location):
    """
    Return an RpmPackage object for the file at location or None if
    the file is not an RPM.
    """
    rpm_tags = get_rpm_tags(location, include_desc=True)
    return build_from_tags(rpm_tags)


def build_from_tags(rpm_tags):
    """
    Return an RpmPackage object from an ``rpm_tags`` RPMtags object.
    """
    if TRACE: logger_debug('build_from_tags: rpm_tags', rpm_tags)
    if not rpm_tags:
        return

    name = rpm_tags.name

    try:
        epoch = rpm_tags.epoch and int(rpm_tags.epoch) or None
    except ValueError:
        epoch = None

    evr = EVR(
        version=rpm_tags.version or None,
        release=rpm_tags.release or None,
        epoch=epoch).to_string()

    qualifiers = {}
    os = rpm_tags.os
    if os and os.lower() != 'linux':
        qualifiers['os'] = os

    arch = rpm_tags.arch
    if arch:
        qualifiers['arch'] = arch

    source_packages = []
    if rpm_tags.source_rpm:
        sepoch, sname, sversion, srel, sarch = nevra.from_name(rpm_tags.source_rpm)
        src_evr = EVR(sversion, srel, sepoch).to_string()
        src_qualifiers = {}
        if sarch:
            src_qualifiers['arch'] = sarch

        src_purl = models.PackageURL(
            type=RpmPackage.default_type,
            name=sname,
            version=src_evr,
            qualifiers=src_qualifiers
        ).to_string()

        if TRACE: logger_debug('build_from_tags: source_rpm', src_purl)
        source_packages = [src_purl]

    parties = []

    # TODO: also use me to craft a namespace!!!
    # TODO: assign a namepsace to Package URL based on distro names.
    # CentOS
    # Fedora Project
    # OpenMandriva Lx
    # openSUSE Tumbleweed
    # Red Hat

    if rpm_tags.distribution:
        parties.append(models.Party(name=rpm_tags.distribution, role='distributor'))

    if rpm_tags.vendor:
        parties.append(models.Party(name=rpm_tags.vendor, role='vendor'))

    description = build_description(rpm_tags.summary, rpm_tags.description)

    if TRACE:
        data = dict(
            name=name,
            version=evr,
            description=description or None,
            homepage_url=rpm_tags.url or None,
            parties=parties,
            declared_license=rpm_tags.license or None,
            source_packages=source_packages,
        )
        logger_debug('build_from_tags: data to create a package:\n', data)

    package = RpmPackage(
        name=name,
        version=evr,
        description=description or None,
        homepage_url=rpm_tags.url or None,
        parties=parties,
        declared_license=rpm_tags.license or None,
        source_packages=source_packages,
    )

    if TRACE:
        logger_debug('build_from_tags: created package:\n', package)

    return package

############################################################################
# FIXME: this license detection code is mostly copied from debian_copyright.py and alpine.py
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
    if declared:
        # normalize spaces
        declared = ' '.join(declared.split())
        return declared


def detect_using_name_mapping(declared, licensing=Licensing()):
    """
    Return a license expression detected from a `declared` license string.
    """
    # TODO: use RPM symbology
    declared = declared.lower()
    detected = get_declared_to_detected().get(declared)
    if detected:
        return str(licensing.parse(detected, simple=True))


_DECLARED_TO_DETECTED = None


def get_declared_to_detected(data_file=None):
    """
    Return a mapping of declared to detected license expression cached and
    loaded from a tab-separated text file, all lowercase, normalized for spaces.

    This data file is from license keys used in RPMs files and should be
    derived from a large collection of RPMs files.
    """
    global _DECLARED_TO_DETECTED
    if _DECLARED_TO_DETECTED is not None:
        return _DECLARED_TO_DETECTED

    _DECLARED_TO_DETECTED = {}
    if not data_file:
        data_file = os.path.join(os.path.dirname(__file__), 'rpm_licenses.txt')
    with open(data_file) as df:
        for line in df:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            decl, _, detect = line.partition('\t')
            if detect and detect.strip():
                decl = decl.strip()
                _DECLARED_TO_DETECTED[decl] = detect
    return _DECLARED_TO_DETECTED
