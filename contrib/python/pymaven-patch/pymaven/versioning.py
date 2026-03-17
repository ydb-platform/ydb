#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


"""
Versioning of artifacts
"""

import functools
import sys

from six.moves import zip_longest
import six

from .errors import RestrictionParseError
from .errors import VersionRangeParseError

if sys.version_info > (2,):
    from .utils import cmp

EXCLUSIVE_CLOSE = ')'
EXCLUSIVE_OPEN = '('
INCLUSIVE_CLOSE = ']'
INCLUSIVE_OPEN = '['

# Known qualifiers, oldest to newest
QUALIFIERS = ["alpha", "beta", "milestone", "rc", "snapshot", "", "sp"]

# Well defined aliases
ALIASES = {
    "ga": "",
    "final": "",
    "cr": "rc",
}


def list2tuple(l):
    return tuple(list2tuple(x) if isinstance(x, list) else x for x in l)


@functools.total_ordering
class Restriction(object):
    """Describes a restriction in versioning
    """
    def __init__(self, spec=None):
        """Create a restriction

        Restrictions are specified using a semi-mathematical notation:

        * 1.0: "Soft" requirement on 1.0 (just a recommendation, if it matches
          all other ranges for the dependency)
        * [1.0]: "Hard" requirement on 1.0
        * (,1.0]: x <= 1.0
        * [1.2,1.3]: 1.2 <= x <= 1.3
        * [1.0,2.0): 1.0 <= x < 2.0
        * [1.5,): x >= 1.5
        * (,1.0],[1.2,): x <= 1.0 or x >= 1.2; multiple sets are comma-separated
        * (,1.1),(1.1,): this excludes 1.1 (for example if it is known not to
          work in combination with this library)

        :param str spec: Restriction specification
        """
        self.lower_bound = None
        self.upper_bound = None
        self.lower_bound_inclusive = False
        self.upper_bound_inclusive = False

        if not spec:
            return

        self.lower_bound_inclusive = (spec.strip()[0] == INCLUSIVE_OPEN)
        self.upper_bound_inclusive = (spec.strip()[-1] == INCLUSIVE_CLOSE)

        _spec = spec[1:-1].strip()
        if ',' in _spec:
            lower_bound, upper_bound = _spec.split(',')
            if lower_bound and lower_bound == upper_bound:
                raise RestrictionParseError(
                    "Range cannot have identical boundaries: %s" % spec)

            self.lower_bound = Version(lower_bound) if lower_bound else None
            self.upper_bound = Version(upper_bound) if upper_bound else None

            if self.lower_bound and self.upper_bound \
                    and self.upper_bound < self.lower_bound:
                raise RestrictionParseError(
                    "Range defies version ordering: %s" % spec)
        else:
            # single version restriction
            if not self.lower_bound_inclusive or not self.upper_bound_inclusive:
                raise RestrictionParseError(
                    "Single version must be surrounded by []: %s" % spec)
            version = Version(_spec)
            self.lower_bound = version
            self.upper_bound = version

    def __contains__(self, version):
        """Return true if version is contained within the restriction

        version must be greater than the lower bound (or equal to it if the
        lower bound is inclusive) and less than the upper bound ( or equal to it
        if the upper bound is inclusive).
        """
        if self.lower_bound:
            if self.lower_bound == version and not self.lower_bound_inclusive:
                return False

            if self.lower_bound > version:
                return False

        if self.upper_bound:
            if self.upper_bound == version and not self.upper_bound_inclusive:
                return False

            if self.upper_bound < version:
                return False

        return True

    def __cmp__(self, other):
        if self is other:
            return 0

        if not isinstance(other, self.__class__):
            if isinstance(other, six.string_types):
                return cmp(self, self.__class__(other))
            return 1

        result = cmp(self.lower_bound, other.lower_bound)
        if result == 0:
            result = cmp(self.lower_bound_inclusive,
                         other.lower_bound_inclusive)
            if result == 0:
                result = cmp(self.upper_bound, other.upper_bound)
                if result == 0:
                    result = cmp(self.upper_bound_inclusive,
                                 other.upper_bound_inclusive)
        return result

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __hash__(self):
        return hash((self.lower_bound, self.lower_bound_inclusive,
                     self.upper_bound, self.upper_bound_inclusive))

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __str__(self):
        s = "{open}{lower}{comma}{upper}{close}".format(
            open=(INCLUSIVE_OPEN if self.lower_bound_inclusive
                  else EXCLUSIVE_OPEN),
            lower=self.lower_bound if self.lower_bound is not None else "",
            comma="," if self.lower_bound != self.upper_bound else "",
            upper=(self.upper_bound if self.upper_bound is not None
                   and self.upper_bound != self.lower_bound else ""),
            close=(INCLUSIVE_CLOSE if self.upper_bound_inclusive
                   else EXCLUSIVE_CLOSE),
            )
        return s

    def __repr__(self):
        return "<%s.%s(%r, %r, %r, %r)>" % (
            self.__module__,
            "Restriction",
            self.lower_bound,
            self.lower_bound_inclusive,
            self.upper_bound,
            self.upper_bound_inclusive,
            )

    @classmethod
    def fromstring(cls, spec):
        return cls(spec)


@functools.total_ordering
class VersionRange(object):
    """Version range specification

    Valid ranges are comma separated range specifications
    """
    def __init__(self, spec):
        """Create a VersionRange from a string specification

        :param spec string representation of a version or version range
        :type spec str
        :return a new VersionRange
        :rtype VersionRange
        :raises RuntimeError if the range is invalid in some way
        """
        restrictions = []
        version = None

        _spec = spec[:]
        lower_bound = None
        upper_bound = None
        while (_spec.startswith(EXCLUSIVE_OPEN)
               or _spec.startswith(INCLUSIVE_OPEN)):
            exclusive_close = _spec.find(EXCLUSIVE_CLOSE)
            inclusive_close = _spec.find(INCLUSIVE_CLOSE)

            close = inclusive_close
            if inclusive_close < 0 or 0 <= exclusive_close < inclusive_close:
                # close is exclusive
                close = exclusive_close

            if close < 0:
                raise VersionRangeParseError("Unbounded range: %s" % spec)

            restriction = Restriction(_spec[0:close+1])

            if lower_bound is None:
                lower_bound = restriction.lower_bound

            if upper_bound is not None:
                if restriction.lower_bound is None \
                        or restriction.lower_bound < upper_bound:
                    raise VersionRangeParseError("Ranges overlap: %s" % spec)
            restrictions.append(restriction)
            upper_bound = restriction.upper_bound

            _spec = _spec[close+1:]
            if _spec and _spec.startswith(','):
                # pop off leading comma
                _spec = _spec[1:]

        if _spec:
            if restrictions:
                raise VersionRangeParseError(
                    "Only fully-qualified sets allowed in multiple set"
                    " scenario: %s" % spec)
            else:
                version = Version(_spec)
                # add the "everything" restriction
                restrictions.append(Restriction())

        self.version = version
        self.restrictions = tuple(restrictions)

    def __cmp__(self, other):
        if self is other:
            return 0

        if not isinstance(other, self.__class__):
            if isinstance(other, six.string_types):
                return cmp(self, self.__class__(other))
            elif isinstance(other, Version):
                return cmp(other, self)
            return 1

        result = cmp(self.version, other.version)
        if result == 0:
            result = cmp(self.restrictions, other.restrictions)

        return result

    def __contains__(self, version):
        return any((version in r) for r in self.restrictions)

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __hash__(self):
        return hash((self.version, self.restrictions))

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __str__(self):
        if self.version:
            return str(self.version)
        else:
            return ','.join(str(r) for r in self.restrictions)

    def __repr__(self):
        return "<%s.%s(%r, %r)>" % (self.__module__, "VersionRange",
                                    self.version, self.restrictions)

    def _intersection(self, l1, l2):
        """Return the intersection of l1 and l2

        :param l1 list of restrictions
        :type l1 [Restriction, ...]
        :param l2 list of restrictions
        :type l2 [Restriction, ...]
        :return Intersection of l1 and l2
        :rtype [Restriction, ...]
        """
        raise NotImplementedError

    @classmethod
    def fromstring(cls, spec):
        return cls(spec)

    @classmethod
    def from_version(cls, version):
        return cls(str(version))

    def restrict(self, version_range):
        """Returns a new VersionRange that is a restriction of this
        and the specified version range.

        Prefers this version over the specified version range

        :param version_range the version range that will restrict this range
        :type version_range VersionRange
        :return intersection of this version range and the specified one
        :rypte VersionRange
        """
        raise NotImplementedError

    def match_version(self, versions):
        matched = None
        for version in sorted(versions, reverse=True):
            if version in self:
                matched = version
                break
        return matched


@functools.total_ordering
class Version(object):
    """Maven version objecjt
    """
    def __init__(self, version):
        """Create a maven version

        The version string is examined one character at a time.

        There's a buffer containing the current text - all characters are
        appended, except for '.' and '-'. When it's stated 'append buffer to
        list', the buffer is first converted to an int if that's possible,
        otherwise left alone as a string. It will only be appended if it's
        length is not 0.

        * If a '.' is encountered, the current buffer is appended to the current
          list, either as a int (if it's a number) or a string.
        * If a '-' is encountered, do the same as when a '.' is encountered,
          then create a new sublist, append it to the current list and replace
          the current list with the new sub-list.
        * If the last character was a digit:
            * and the current one is too, append it to the buffer.
            * otherwise append the current buffer to the list, reset the buffer
              with the current char as content
        * if the last character was NOT a digit:
            * if the current character is also NOT a digit, append it to the
              buffer
            * if it is a digit, append buffer to list, set buffers content to
              the digit
        * finally, append the buffer to the list
        """
        self._unparsed = version
        parsed = current_list = []
        buf = str(version.strip()).lower()
        start = 0
        is_digit = False
        for idx, ch in enumerate(buf):
            if ch == '.':
                if idx == start:
                    current_list.append(0)
                else:
                    current_list.append(self._parse_buffer(buf[start:idx]))
                start = idx + 1
            elif ch == '-':
                if idx == start:
                    current_list.append(0)
                else:
                    current_list.append(self._parse_buffer(buf[start:idx]))
                start = idx + 1
                current_list = self._new_list(current_list)
            elif ch.isdigit():
                if not is_digit and idx > start:
                    current_list.append(
                        self._parse_buffer(buf[start:idx], True))
                    current_list = self._new_list(current_list)
                    start = idx
                is_digit = True
            else:
                if is_digit and idx > start:
                    current_list.append(self._parse_buffer(buf[start:idx]))
                    current_list = self._new_list(current_list)
                    start = idx
                is_digit = False
        else:
            if len(buf) > start:
                current_list.append(self._parse_buffer(buf[start:]))
        current_list = self._normalize(current_list)

        self._parsed = list2tuple(self._normalize(parsed))

    def __cmp__(self, other):
        if self is other:
            return 0

        if not isinstance(other, self.__class__):
            if isinstance(other, six.string_types):
                return self._compare(self._parsed, self.__class__(other)._parsed)
            elif isinstance(other, VersionRange) and other.version:
                return self._compare(self._parsed, other.version._parsed)
            return 1

        return self._compare(self._parsed, other._parsed)

    def __eq__(self, other):
        return self.__cmp__(other) == 0

    def __hash__(self):
        return hash(self._unparsed)

    def __lt__(self, other):
        return self.__cmp__(other) < 0

    def __ne__(self, other):
        return self.__cmp__(other) != 0

    def __repr__(self):
        return "<%s.%s(%r)>" % (self.__module__, "Version", self._unparsed)

    def __str__(self):
        return self._unparsed

    def _compare(self, this, other):
        if isinstance(this, int):
            return self._int_compare(this, other)
        elif isinstance(this, six.string_types):
            return self._string_compare(this, other)
        elif isinstance(this, (list, tuple)):
            return self._list_compare(this, other)
        else:
            raise RuntimeError("Unknown type for t: %r" % this)

    def _int_compare(self, this, other):
        if isinstance(other, int):
            return this - other
        elif isinstance(other, (six.string_types, list, tuple)):
            return 1
        elif other is None:
            return this
        else:
            raise RuntimeError("other is of invalid type: %s" % type(other))

    def _list_compare(self, l, other):
        if other is None:
            if len(l) == 0:
                return 0
            return self._compare(l[0], other)
        if isinstance(other, int):
            return -1
        elif isinstance(other, six.string_types):
            return 1
        elif isinstance(other, (list, tuple)):
            for left, right in zip_longest(l, other):
                if left is None:
                    if right is None:
                        result = 0
                    else:
                        result = -1 * self._compare(right, left)
                else:
                    result = self._compare(left, right)
                if result != 0:
                    return result
            else:
                return 0
        else:
            raise RuntimeError("other is of invalid type: %s" % type(other))

    def _new_list(self, l):
        """Create a new sublist, append it to the current list and return the
        sublist

        :param list l: list to add a sublist to
        :return: the sublist
        :rtype: list
        """
        l = self._normalize(l)
        sublist = []
        l.append(sublist)
        return sublist

    def _normalize(self, l):
        for item in l[::-1]:
            if not item:
                l.pop()
            elif not isinstance(item, list):
                break
        return l

    def _string_compare(self, s, other):
        """Compare string item `s` to `other`

        :param str s: string item to compare
        :param other: other item to compare
        :type other: int, str, list or None
        """
        if other is None:
            return self._string_compare(s, "")

        if isinstance(other, (int, list, tuple)):
            return -1
        elif isinstance(other, six.string_types):
            s_value = self._string_value(s)
            other_value = self._string_value(other)
            if s_value < other_value:
                return -1
            elif s_value == other_value:
                return 0
            else:
                return 1
        else:
            raise RuntimeError("other is of invalid type: %s" % type(other))

    def _parse_buffer(self, buf, followed_by_digit=False):
        """Parse the string buf to determine if it is string or an int

        :param str buf: string to parse
        :param bool followed_by_digit: s is followed by a digit, eg. 'a1'
        :return: integer or string value of buf
        :rtype: int or str
        """
        if buf.isdigit():
            buf = int(buf)
        elif followed_by_digit and len(buf) == 1:
            if buf == 'a':
                buf = 'alpha'
            elif buf == 'b':
                buf = 'beta'
            elif buf == 'm':
                buf = 'milestone'

        return ALIASES.get(buf, buf)

    def _string_value(self, s):
        """Convert a string into a comparable value.

        If the string is a known qualifier, or an alias of a known qualifier,
        then return its index in the QUALIFIERS list. Otherwise return a string
        of the length of the QUALIFIERS list - s, eg. 7-foo

        :param str s: string to convert into value
        :return: value of string `s`
        :rtype: str
        """
        if s in QUALIFIERS:
            return str(QUALIFIERS.index(s) + 1)

        return "%d-%s" % (len(QUALIFIERS), s)

    @classmethod
    def fromstring(cls, spec):
        return cls(spec)
