#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import logging
import os
import posixpath
import sys

import xmltodict

from commoncode import command
from packagedcode import models
from textcode.analysis import as_unicode

TRACE = False


def logger_debug(*args):
    pass


logger = logging.getLogger(__name__)

if TRACE:
    logging.basicConfig(stream=sys.stdout)
    logger.setLevel(logging.DEBUG)

    def logger_debug(*args):
        return logger.debug(' '.join(isinstance(a, str) and a or repr(a) for a in args))


def parse_rpm_xmlish(location, detect_licenses=False):
    """
    Yield RpmPackage(s) from a RPM XML'ish file at `location`. This is a file
    created with rpm and the xml query option.
    """
    if not location or not os.path.exists(location):
        return

    # there are smetimes weird encodings. We avoid issues there
    with open(location, 'rb') as f:
        rpms = as_unicode(f.read())

    # The XML'ish format is in fact multiple XML documents, one for each package
    # wrapped in an <rpmHeader> root element. So we add a global root element
    # to make this a valid XML document.

    for rpm_raw_tags in collect_rpms(rpms):
        tags = collect_tags(rpm_raw_tags)
        pkg = build_package(tags)
        if detect_licenses:
            pkg.license_expression = pkg.compute_normalized_license()
        yield pkg


def collect_rpms(text):
    """
    Yield lists of RPM raw tags, one list for each RPM from an XML'ish ``text``.

    The XML'ish format is in fact multiple XML documents, one for each package
    wrapped in an <rpmHeader> root element. Therefore, we add a global root
    element to make this a valid XML document before parsing it.

    Each XML document represents one RPM package and has this overall shape:
        <rpmHeader>
        ....
          <rpmTag name="Name"><string>perl-Errno</string></rpmTag>
          <rpmTag name="Version"><string>1.30</string></rpmTag>
        ......
          <rpmTag name="License"><string>GPL+ or Artistic</string></rpmTag>
        ....
          <rpmTag name="Filesizes">
            <integer>6863</integer>
            <integer>2629</integer>
          </rpmTag>
          <rpmTag name="Filemodes">
            <integer>33188</integer>
            <integer>33188</integer>
          </rpmTag>
        ...
        </rpmHeader>

    After wrapping in a top level element we get this shape:
    <rpms>
        <rpmHeader/>
        <rpmHeader/>
    </rpms>

    When parsed with xmltodict we end up with this structure:
    {'rpms':  {'rpmHeader': [
        {'rpmTag': [
            {'@name': 'Name', 'string': 'boost-license1_71_0'},
            {'@name': 'Version', 'string': '1.71.0'},
            {'@name': 'Filesizes', 'integer': ['0', '1338']},
            {'@name': 'Filestates', 'integer': ['0', '0']},
        ]},
        {'rpmTag': [
            {'@name': 'Name', 'string': 'foobar'},
            {'@name': 'Version', 'string': '1.0'},
            ...
        ]},
    }}

    Some of these structures are peculiar as there are parallel lists (for
    instance for files) and each name comes with a type.
    """
    text = f'<rpms>{text}</rpms>'
    parsed = xmltodict.parse(text, dict_constructor=dict)
    rpms = parsed['rpms']['rpmHeader']
    for rpm in rpms:
        yield rpm['rpmTag']


def collect_tags(raw_tags):
    """
    Yield tags as (name, value_type, value) tubles from a ``raw_tags`` list of
    raw RPM tag data.

    """
    for rtag in raw_tags:
        # The format of a raw tag is: ('@name', 'sample'), ('string', 'sample context')
        name = rtag.pop('@name')
        assert len(rtag) == 1
        value_type, value = list(rtag.items())[0]
        yield name, value_type, value


def build_package(rpm_tags):
    """
    Return an RpmBasePackage object from an `rpm_tags` iterable of (name,
    value_type, value) tuples.
    """
    # guard from circular import
    from packagedcode.rpm import RpmPackage

    # mapping of real Package field name -> value converted to expected format
    converted = {}
    for name, value_type, value in rpm_tags:
        handler = handler_by_name.get(name)
        # FIXME: we need to handle EVRA correctly
        # TODO: add more fields
        # TODO: mereg with tag handling in rpm.py
        if handler:
            handled = handler(value, **converted)
            converted.update(handled)

    # construct the package: we ignore unknown as we added a few technical fields
    package = RpmPackage.create(**converted)
    return package

################################################################################
# Each handler function accepts a value and returns a {name: value} mapping
# Note handlers MUST accept **kwargs as they also receive the current data
# being processed so far as a processed_data kwarg, but most do not use it


def name_value_str_handler(name):
    """
    Return a generic handler for plain string fields.
    """

    def handler(value, **kwargs):
        return {name: value}

    return handler


def size_handler(value, **kwargs):
    return {'size': int(value)}


def arch_handler(value, **kwargs):
    """
    Return a Package URL qualifier for the arch.
    """
    return {'qualifiers': 'arch={}'.format(value)}


def checksum_handler(value, **kwargs):
    """
    Return a list which contains the MD5 hash
    """
    return {'current_file': value}


def dir_index_handler(value, current_file, **kwargs):
    """
    Return a list of tuples with (dirindexes, md5)
    """
    return {'current_file': list(zip(value, current_file))}


def basename_handler(value, current_file, **kwargs):
    """
    Return a list of tuples with (dirindexes, md5, basename)
    """
    data = []
    for index, file in enumerate(current_file):
        basename = (value[index],)
        data.append(file + basename)
    return {'current_file': data}


def dirname_handler(value, current_file, **kwargs):
    """
    Update the current_file by adding the correct dir and basename
    along with the md5 value. Add to installed_files
    """
    installed_files = []
    for file in current_file:
        dirindexes, md5, basename = file
        dirname = value[int(dirindexes)]
        # TODO: review this. Empty filename does not make sense, unless these
        # are directories that we might ignore OK.

        # There is case where entry of basename is "</string>" which will
        # cause error as None type cannot be used for join.
        # Therefore, we need to convert the None type to empty string
        # in order to make the join works.
        if basename == None:
            basename = ''

        rpm_file = models.PackageFile(
            path=posixpath.join(dirname, basename),
            md5=md5,
        )
        installed_files.append(rpm_file)

    return {'installed_files': installed_files}


# mapping of:
# - the package field one letter name in the installed db,
# - an handler for that field
handler_by_name = {

    ############################################################################
    # per-package fields
    ############################################################################

    'Name': name_value_str_handler('name'),
    # TODO: add these
    #  'Epoch'
    #  'Release'
    'Version': name_value_str_handler('version'),
    'Description': name_value_str_handler('description'),
    'Sha1header': name_value_str_handler('sha1'),
    'Url': name_value_str_handler('homepage_url'),
    'License': name_value_str_handler('declared_license'),
    'Arch':  arch_handler,
    'Size': size_handler,

    ############################################################################
    # per-file fields
    ############################################################################
    # TODO: these two are needed:
    #  'Fileflags' -> contains if a file is doc or license
    #  'Filesizes' -> useful info

    'Filedigests': checksum_handler,
    'Dirindexes': dir_index_handler,
    'Basenames': basename_handler,
    'Dirnames': dirname_handler,
    ############################################################################

    ############################################################################
    # TODO: ignored per-package fields. from here on, these fields are not used yet
    ############################################################################
    #  '(unknown)'
    #  'Archivesize'
    #  'Buildhost'
    #  'Buildtime'
    #  'Changelogname'
    #  'Changelogtext'
    #  'Changelogtime'
    #  'Classdict'
    #  'Conflictflags'
    #  'Conflictname'
    #  'Conflictversion'
    #  'Cookie'
    #  'Dependsdict'
    #  'Distribution'
    #  'Dsaheader'
    #  'Fileclass'
    #  'Filecolors'
    #  'Filecontexts'
    #  'Filedependsn'
    #  'Filedependsx'
    #  'Filedevices'
    #  'Filegroupname'
    #  'Fileinodes'
    #  'Filelangs'
    #  'Filelinktos'
    #  'Filemodes'
    #  'Filemtimes'
    #  'Filerdevs'
    #  'Filestates'
    #  'Fileusername'
    #  'Fileverifyflags'
    #  'Group'
    #  'Installcolor'
    #  'Installtid'
    #  'Installtime'
    #  'Instprefixes'
    #  'Obsoleteflags'
    #  'Obsoletename'
    #  'Obsoleteversion'
    #  'Optflags'
    #  'Os'
    #  'Packager'
    #  'Payloadcompressor'
    #  'Payloadflags'
    #  'Payloadformat'
    #  'Platform'
    #  'Postin'
    #  'Postinprog'
    #  'Postun'
    #  'Postunprog'
    #  'Prefixes'
    #  'Prein'
    #  'Preinprog']
    #  'Preun'
    #  'Preunprog'
    #  'Provideflags'
    #  'Providename'
    #  'Provideversion'
    #  'Requireflags'
    #  'Requirename'
    #  'Requireversion'
    #  'Rpmversion'
    #  'Sigmd5'
    #  'Sigsize'
    #  'Sourcerpm'
    #  'Summary'
    #  'Triggerflags'
    #  'Triggerindex'
    #  'Triggername'
    #  'Triggerscriptprog'
    #  'Triggerscripts'
    #  'Triggerversion'
    #  'Vendor'
    #  'Headeri18ntable'
    ############################################################################

}

RPM_BIN_DIR = 'rpm_inspector_rpm.rpm.bindir'


def get_rpm_bin_location():
    """
    Return the binary location for an RPM exe loaded from a plugin-provided path.
    """
    from plugincode.location_provider import get_location
    rpm_bin_dir = get_location(RPM_BIN_DIR)
    if not rpm_bin_dir:
        raise Exception(
            'CRITICAL: RPM executable is not provided. '
            'Unable to continue: you need to install a valid rpm-inspector-rpm '
            'plugin with a valid RPM executable and shared libraries available.'
    )

    return rpm_bin_dir


class InstalledRpmError(Exception):
    pass


def collect_installed_rpmdb_xmlish_from_rootfs(root_dir):
    """
    Return the location of an RPM "XML'ish" inventory file collected from the
    ``root_dir`` rootfs directory or None.

    Raise an InstalledRpmError exception on errors.

    The typical locations of the rpmdb are:

    /var/lib/rpm/
        centos all versions and rpmdb formats
        fedora all versions and rpmdb formats
        openmanidriva all versions and rpmdb formats
        suse/opensuse all versions using bdb rpmdb format

    /usr/lib/sysimage/rpm/ (/var/lib/rpm/ links to /usr/lib/sysimage/rpm)
        suse/opensuse versions that use ndb rpmdb format
    """
    root_dir = os.path.abspath(os.path.expanduser(root_dir))

    rpmdb_loc = os.path.join(root_dir, 'var/lib/rpm')
    if not os.path.exists(rpmdb_loc):
        rpmdb_loc = os.path.join(root_dir, 'usr/lib/sysimage/rpm')
        if not os.path.exists(rpmdb_loc):
            return
    return collect_installed_rpmdb_xmlish_from_rpmdb_loc(rpmdb_loc)


def collect_installed_rpmdb_xmlish_from_rpmdb_loc(rpmdb_loc):
    """
    Return the location of an RPM "XML'ish" inventory file collected from the
    ``rpmdb_loc`` rpmdb directory or None.

    Raise an InstalledRpmError exception on errors.
    """
    rpmdb_loc = os.path.abspath(os.path.expanduser(rpmdb_loc))
    if not os.path.exists(rpmdb_loc):
        return
    rpm_bin_dir = get_rpm_bin_location()

    env = dict(os.environ)
    env['RPM_CONFIGDIR'] = rpm_bin_dir
    env['LD_LIBRARY_PATH'] = rpm_bin_dir

    args = [
        '--query',
        '--all',
        '--qf', '[%{*:xml}\n]',
        '--dbpath', rpmdb_loc,
    ]

    cmd_loc = os.path.join(rpm_bin_dir, 'rpm')
    if TRACE:
        full_cmd = ' '.join([cmd_loc] + args)
        logger_debug(
            f'collect_installed_rpmdb_xmlish_from_rpmdb_loc:\n'
            f'cmd: {full_cmd}')

    rc, stdout_loc, stderr_loc = command.execute2(
        cmd_loc=cmd_loc,
        args=args,
        lib_dir=rpm_bin_dir,
        env=env,
        to_files=True,
    )

    if TRACE:
        full_cmd = ' '.join([cmd_loc] + args)
        logger_debug(
            f'collect_installed_rpmdb_xmlish_from_rpmdb_loc:\n'
            f'cmd: {full_cmd}\n'
            f'rc: {rc}\n'
            f'stderr: file://{stderr_loc}\n'
            f'stdout: file://{stdout_loc}\n')

    if rc != 0:
        with open(stderr_loc) as st:
            stde = st.read()
        full_cmd = ' '.join([cmd_loc] + args)
        msg = f'collect_installed_rpmdb_xmlish_from_rpmdb_loc: Failed to execute RPM command: {full_cmd}\n{stde}'
        raise Exception(msg)

    return stdout_loc
