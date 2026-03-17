#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
# Copyright (c) 2018 Peter Odding
# Author: Peter Odding <peter@peterodding.com>
# URL: https://github.com/xolox/python-deb-pkg-tools
#

import itertools
from os import path

from attr import attrs
from attr import attrib

from debian_inspector.version import Version

"""
Functions to build and inspect Debian binary package archives (``*.deb``,
``*.udeb`` files as well ``.orig`` and ``.debian`` tarballs).
"""


@attrs
class DebArchive(object):
    """
    A .deb binary package archive.
    """

    name = attrib()
    version = attrib()
    architecture = attrib()
    original_filename = attrib(default=None)

    @classmethod
    def from_filename(cls, filename):
        """
        Parse the filename of a Debian binary package archive and return a
        DebArchive instance. Raise ValueError if the `filename` is not valid.
        """
        if isinstance(filename, DebArchive):
            return filename

        name, version, architecture = get_nva(path.basename(filename))
        return cls(
            name=name, version=version, architecture=architecture, original_filename=filename
        )

    def to_dict(self):
        data = {}
        data["name"] = self.name
        data["version"] = self.version
        data["architecture"] = self.architecture
        data["original_filename"] = self.original_filename
        return data

    def to_tuple(self):
        """
        Return a tuple of name, Version, architecture suitable for sorting.
        This tuple does not contain the original_filename value.
        """
        return tuple(v for v in self.to_dict().values() if v != "original_filename")


@attrs
class CodeArchive(object):
    """
    A Debian .orig or .debian package archive.
    These are not architecture-specific.
    For instance in ./pool/main/a/apr-util there are files such as:

    - apr-util_1.5.4.orig.tar.bz2 that contains the original upstream code
    - apr-util_1.6.1-4.debian.tar.xz that contains the Debian patches and
      control files
    """

    name = attrib()
    version = attrib()
    original_filename = attrib(default=None)

    @classmethod
    def from_filename(cls, filename):
        """
        Parse the `filename` of a Debian original package archive and return an OrigArchive instance.
        Raise ValueError if the `filename` is not valid.
        """
        if isinstance(filename, CodeArchive):
            return filename

        name, version, _architecture = get_nva(path.basename(filename))
        return cls(name=name, version=version, original_filename=filename)

    def to_dict(self):
        data = {}
        data["name"] = self.name
        data["version"] = self.version
        data["original_filename"] = self.original_filename
        return data

    def to_tuple(self):
        """
        Return a tuple of name, Vresion, architecture suitable for sorting.
        This tuple does not contain the original_filename values.
        """
        return tuple(v for v in self.to_dict().values() if v != "original_filename")


@attrs
class CodeMetadata(CodeArchive):
    """
    A .dsc, copyright or changelog file present in the debian
    package/metadata archive and contains package information
    on the filename or as file contents.

    For example in ./changelogs/main/d/diffutils/ there are
    files such as:
    - diffutils_3.7-5_copyright
    - diffutils_3.7-5_changelog
    And in .pool/main/b/base-files/ there are files such as:
    - base-files_11.1+deb11u8.dsc
    """


def get_nva(filename):
    """
    Return a tuple of (name string, Version object, archictecture string or
    None) parsed from the `filename` of .deb, .udeb, .orig or .debian archive..
    """
    is_known = False
    if filename.endswith((".deb", ".udeb", ".dsc")):
        basename, _extension = path.splitext(filename)
        is_known = True

    elif filename.endswith(("_changelog", "_copyright")):
        basename, _, _ = filename.rpartition("_")
        is_known = True

    elif filename.endswith((".tar.gz", ".tar.xz", ".tar.bz2", ".tar.lzma")):
        # A Format: 3.0 archive.
        # Note that we ignore the legacy .diff.gz files for Format: 1.0
        basename, _, _ = filename.rpartition(".tar.")
        # remove the .orig or .debian
        basename, pkgtype = path.splitext(basename)
        if pkgtype in (".orig", ".debian"):
            is_known = True

    if not is_known:
        raise ValueError("Unknown Debian archive filename format: {}".format(filename))

    parts = basename.split("_")
    if len(parts) == 2:
        arch = None
        name, evr = parts

    elif len(parts) == 3:
        name, evr, arch = parts

    else:
        raise ValueError("Unknown Debian archive filename format: {}".format(filename))

    return name, Version.from_string(evr), arch


# TODO: simplify me
def match_relationships(package_archive, relationship_sets):
    """
    Validate that `package_archive` DebArchive satisfies all the relationships
    of a `relationship_sets`. Return True if valid and False otherwise.
    """
    archive_matches = None
    for relationships in relationship_sets:
        status = relationships.matches(package_archive.name, package_archive.version)
        if status is True and archive_matches is not False:
            archive_matches = True
        elif status is False:
            # This package archive specifically conflicts with (at least) one
            # of the given relationship sets.
            archive_matches = False
            # Short circuit the evaluation of further relationship sets because
            # we've already found our answer.
            break
    return archive_matches


def find_latest_version(packages):
    """
    Return the DebArchive package archive with the highest version number given
    a `packages` list of  package filename strings or DebArchive instances.
    All package items must have the same package name

    Raise ValueError if a package filename is invalid or if the packages do not
    have all the same package name.
    """
    if not packages:
        return
    packages = [DebArchive.from_filename(fn) for fn in packages]
    packages = sorted(packages, key=lambda p: p.to_tuple())
    names = set(p.name for p in packages)
    if len(names) > 1:
        msg = "Cannot compare versions of different package names"
        raise ValueError(msg.format(" ".join(sorted(names))))
    return packages[-1]


def find_latest_versions(packages):
    """
    Return a mapping {name: DebArchive} where DebArchive is the package archive
    with the highest version number for a given package name given a `packages`
    list of  package filename strings or DebArchive instances.
    """
    if not packages:
        return
    packages = sorted([DebArchive.from_filename(fn) for fn in packages])
    latests = {}
    for name, packages_group in itertools.groupby(packages, key=lambda p: p.name):
        latest = find_latest_version(packages_group)
        latests[name] = latest
    return latests
