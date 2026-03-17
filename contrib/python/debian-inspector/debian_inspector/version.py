#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: MIT
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
# Copyright (c) Peter Odding
# Author: Peter Odding <peter@peterodding.com>
# URL: https://github.com/xolox/python-deb-pkg-tools
#

import logging
import operator as operator_module
import re
from functools import cmp_to_key
from itertools import zip_longest

from attr import asdict
from attr import attrs
from attr import attrib

logger = logging.getLogger(__name__)

"""
Parse, compare and sort Debian package versions.

This module is an implementation of the version comparison and sorting algorithm
described at
https://www.debian.org/doc/debian-policy/ch-controlfields.html#version

This has been substantially modified and enhanced from the original python-dpkg
Dpkg class by Nathan J. Meh and team from The Climate Corporation and
code from python-deb-pkg-tools by Peter Odding to extract only the subset that
does the version parsing, comparison and version constraints evaluatiob.


So much so that little of this code may still looks like the original.

Some examples:

#### Compare two arbitrary version strings

    >>> from debian_inspector import debver
    >>> debver.compare_versions('0:1.0-test1', '0:1.0-test2')
    -1
    >>> debver.compare_versions('1.0', '0.6')
    1
    >>> debver.compare_versions('2:1.0', '1:1.0')
    -1

#### Use Version as a key function to sort a list of version strings

    >>> from debian_inspector.debver import Version
    >>> sorted(['0:1.0-test1', '1:0.0-test0', '0:1.0-test2'] , key=Version.from_string)
    ['0:1.0-test1', '0:1.0-test2', '1:0.0-test0']

"""


@attrs(eq=False, order=False, frozen=True, hash=False, slots=True, str=False)
class Version(object):
    """
    Rich comparison of Debian package versions as first-class Python objects.

    The :class:`Version` class is a subclass of the built in :class:`str` type
    that implements rich comparison according to the version sorting order
    defined in the Debian Policy Manual. Use it to sort Debian package versions
    from oldest to newest in ascending version order like this:

      >>> from debian_inspector.version import Version
      >>> unsorted = ['0.1', '0.5', '1.0', '2.0', '3.0', '1:0.4', '2:0.3']
      >>> print([str(v) for v in sorted(Version.from_string(s) for s in unsorted)])
      ['0.1', '0.5', '1.0', '2.0', '3.0', '1:0.4', '2:0.3']

    This example uses 'epoch' numbers (the numbers before the colons) to
    demonstrate that this version sorting order is different from regular
    sorting and 'natural order sorting'.
    """

    epoch = attrib(default=0)
    upstream = attrib(default=None)
    revision = attrib(default="0")

    def __str__(self, *args, **kwargs):
        if self.epoch:
            version = f"{self.epoch}:{self.upstream}"
        else:
            version = f"{self.upstream}"

        if self.revision not in (None, "0"):
            version += f"-{self.revision}"

        return version

    def __repr__(self, *args, **kwargs):
        return str(self)

    def __hash__(self):
        return hash(self.tuple())

    def __eq__(self, other):
        return type(self) is type(other) and self.tuple() == other.tuple()

    def __ne__(self, other):
        return not self.__eq__(other)

    def __lt__(self, other):
        if type(self) is type(other):
            return eval_constraint(self, "<<", other)
        return NotImplemented

    def __le__(self, other):
        if type(self) is type(other):
            return eval_constraint(self, "<=", other)
        return NotImplemented

    def __gt__(self, other):
        if type(self) is type(other):
            return eval_constraint(self, ">>", other)
        return NotImplemented

    def __ge__(self, other):
        if type(self) is type(other):
            return eval_constraint(self, ">=", other)
        return NotImplemented

    @classmethod
    def from_string(cls, version):
        if not version and not isinstance(version, str):
            raise ValueError('Invalid version string: "{}"'.format(version))
        version = version.strip()
        if not version:
            raise ValueError('Invalid version string: "{}"'.format(version))
        if not _is_valid_version(version):
            raise ValueError('Invalid version string: "{}"'.format(version))

        if ":" in version:
            epoch, _, version = version.partition(":")
            epoch = int(epoch)
        else:
            epoch = 0

        if "-" in version:
            upstream, _, revision = version.rpartition("-")
        else:
            upstream = version
            revision = "0"
        return cls(epoch=epoch, upstream=upstream, revision=revision)

    def compare(self, other_version):
        return compare_versions(self, other_version)

    def to_dict(self):
        return asdict(self)

    def tuple(self):
        return self.epoch, self.upstream, self.revision


_is_valid_version = re.compile(
    r"^"
    # epoch must start with a digit
    r"(\d+:)?"
    # upstream must start with a digit
    r"\d"
    r"("
    # upstream  can contain only alphanumerics and the characters . + -
    # ~ (full stop, plus, hyphen, tilde)
    # we are adding the extra check that it must end with alphanum
    r"[A-Za-z0-9\.\+\-\~]*[A-Za-z0-9]"
    r"|"
    # If there is no debian_revision then hyphens are not allowed.
    # we are adding the extra check that it must end with alphanum
    r"[A-Za-z0-9\.\+\~]*[A-Za-z0-9]-[A-Za-z0-9\+\.\~]*[A-Za-z0-9\~]"
    r")?"
    r"$"
).match


def eval_constraint(version1, operator, version2):
    """
    Evaluate a versions constraint where two Debian package versions are
    compared with an operator such as < or >. Return True if the constraint is
    satisfied and False otherwise.
    """

    version1 = coerce_version(version1)
    version2 = coerce_version(version2)

    result = compare_versions(version1, version2)
    # See https://www.debian.org/doc/debian-policy/ch-relationships.html#syntax-of-relationship-fields
    operators = {
        "<=": operator_module.le,
        # legacy for compat
        "<": operator_module.le,
        ">=": operator_module.ge,
        # legacy for compat
        ">": operator_module.ge,
        "<<": operator_module.lt,
        ">>": operator_module.gt,
        "=": operator_module.eq,
    }

    try:
        operator = operators[operator]
    except KeyError:
        msg = f"Unsupported Debian version constraint comparison operator: {version1} {operator} {version2}"
        raise ValueError(msg)
    return operator(result, 0)


def compare_versions_key(x):
    """
    Return a key version function suitable for use in sorted().
    """
    return cmp_to_key(compare_versions)(x)


def compare_strings_key(x):
    """
    Return a key string function suitable for use in sorted().
    """
    return cmp_to_key(compare_strings)(x)


def compare_strings(version1, version2):
    """
    Compare two version strings (upstream or revision) using Debain semantics
    and return one of the following integer numbers:

        - -1 means version1 sorts before version2
        - 0 means version1 and version2 are equal
        - 1 means version1 sorts after version2
    """
    logger.debug("Comparing Debian version number substrings %r and %r ..", version1, version2)
    mapping = characters_order
    v1 = list(version1)
    v2 = list(version2)
    while v1 or v2:
        # Quoting from the 'deb-version' manual page: First the initial part of each
        # string consisting entirely of non-digit characters is determined. These two
        # parts (one of which may be empty) are compared lexically. If a difference is
        # found it is returned. The lexical comparison is a comparison of ASCII values
        # modified so that all the letters sort earlier than all the non-letters and so
        # that a tilde sorts before anything, even the end of a part. For example, the
        # following parts are in sorted order: '~~', '~~a', '~', the empty part, 'a'.
        p1 = get_non_digit_prefix(v1)
        p2 = get_non_digit_prefix(v2)
        if p1 != p2:
            logger.debug("Comparing non-digit prefixes %r and %r ..", p1, p2)
            for c1, c2 in zip_longest(p1, p2, fillvalue=""):
                logger.debug(
                    "Performing lexical comparison between characters %r and %r ..", c1, c2
                )
                o1 = mapping.get(c1)
                o2 = mapping.get(c2)
                if o1 < o2:
                    logger.debug(
                        "Determined that %r sorts before %r (based on lexical comparison).",
                        version1,
                        version2,
                    )
                    return -1
                elif o1 > o2:
                    logger.debug(
                        "Determined that %r sorts after %r (based on lexical comparison).",
                        version1,
                        version2,
                    )
                    return 1
        elif p1:
            logger.debug("Skipping matching non-digit prefix %r ..", p1)
        # Quoting from the 'deb-version' manual page: Then the initial part of the
        # remainder of each string which consists entirely of digit characters is
        # determined. The numerical values of these two parts are compared, and any
        # difference found is returned as the result of the comparison. For these purposes
        # an empty string (which can only occur at the end of one or both version strings
        # being compared) counts as zero.
        d1 = get_digit_prefix(v1)
        d2 = get_digit_prefix(v2)
        logger.debug("Comparing numeric prefixes %i and %i ..", d1, d2)
        if d1 < d2:
            logger.debug(
                "Determined that %r sorts before %r (based on numeric comparison).",
                version1,
                version2,
            )
            return -1
        elif d1 > d2:
            logger.debug(
                "Determined that %r sorts after %r (based on numeric comparison).",
                version1,
                version2,
            )
            return 1
        else:
            logger.debug("Determined that numeric prefixes match.")
    logger.debug("Determined that version numbers are equal.")
    return 0


def compare_versions(version1, version2):
    """
    Compare two Version objects or strings and return one of the following
    integer numbers:

      - -1 means version1 sorts before version2
      - 0 means version1 and version2 are equal
      - 1 means version1 sorts after version2
    """
    version1 = coerce_version(version1)
    version2 = coerce_version(version2)
    return compare_version_objects(version1, version2)


def coerce_version(value):
    """
    Return a Version object from value.

    :param value: The value to coerce (a string or :class:`Version` object).
    :returns: A :class:`Version` object.
    """
    if not isinstance(value, Version):
        value = Version.from_string(value)
    return value


def compare_version_objects(version1, version2):
    """
    Compare two Version objects and return one of the following
    integer numbers:

      - -1 means version1 sorts before version2
      - 0 means version1 and version2 are equal
      - 1 means version1 sorts after version2
    """
    if version1.epoch < version2.epoch:
        return -1
    if version1.epoch > version2.epoch:
        return 1
    result = compare_strings(version1.upstream, version2.upstream)
    if result != 0:
        return result
    if version1.revision or version2.revision:
        return compare_strings(version1.revision, version2.revision)
    return 0


def get_digit_prefix(characters):
    """
    Return the digit prefix from a list of characters.
    """
    value = 0
    while characters and characters[0].isdigit():
        value = value * 10 + int(characters.pop(0))
    return value


def get_non_digit_prefix(characters):
    """
    Return the non-digit prefix from a list of characters.
    """
    prefix = []
    while characters and not characters[0].isdigit():
        prefix.append(characters.pop(0))
    return prefix


# a mapping of characters to integers representing the Debian sort order.
characters_order = {
    # The tilde sorts before everything.
    "~": 0,
    # The empty string sort before everything except a tilde.
    "": 1,
    # Letters sort before everything but a tilde or empty string, in their regular lexical sort order.
    "A": 2,
    "B": 3,
    "C": 4,
    "D": 5,
    "E": 6,
    "F": 7,
    "G": 8,
    "H": 9,
    "I": 10,
    "J": 11,
    "K": 12,
    "L": 13,
    "M": 14,
    "N": 15,
    "O": 16,
    "P": 17,
    "Q": 18,
    "R": 19,
    "S": 20,
    "T": 21,
    "U": 22,
    "V": 23,
    "W": 24,
    "X": 25,
    "Y": 26,
    "Z": 27,
    "a": 28,
    "b": 29,
    "c": 30,
    "d": 31,
    "e": 32,
    "f": 33,
    "g": 34,
    "h": 35,
    "i": 36,
    "j": 37,
    "k": 38,
    "l": 39,
    "m": 40,
    "n": 41,
    "o": 42,
    "p": 43,
    "q": 44,
    "r": 45,
    "s": 46,
    "t": 47,
    "u": 48,
    "v": 49,
    "w": 50,
    "x": 51,
    "y": 52,
    "z": 53,
    # Punctuation characters follow in their regular lexical sort order.
    "+": 54,
    "-": 55,
    ".": 56,
}
