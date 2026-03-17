# Copyright (C) 2005-2007 Jelmer Vernooij <jelmer@jelmer.uk>

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation; either version 2.1 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Handling of Subversion properties."""

__author__ = "Jelmer Vernooij <jelmer@jelmer.uk>"
__docformat__ = "restructuredText"


import bisect
import calendar
import time
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse


class InvalidExternalsDescription(Exception):
    _fmt = """Unable to parse externals description."""


def is_valid_property_name(prop):
    """Check the validity of a property name.

    :param prop: Property name
    :return: Whether prop is a valid property name
    """
    if not prop[0].isalnum() and not prop[0] in ":_":
        return False
    for c in prop[1:]:
        if not c.isalnum() and c not in "-:._":
            return False
    return True


def time_to_cstring(timestamp):
    """Determine string representation of a time.

    :param timestamp: Number of microseconds since the start of 1970
    :return: string with date
    """
    tm_usec = timestamp % 1000000
    (tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec, tm_wday, tm_yday,
     tm_isdst) = time.gmtime(timestamp / 1000000)
    return "%04d-%02d-%02dT%02d:%02d:%02d.%06dZ" % (
            tm_year, tm_mon, tm_mday, tm_hour, tm_min, tm_sec, tm_usec)


def time_from_cstring(text):
    """Parse a time from a cstring.

    :param text: Parse text
    :return: number of microseconds since the start of 1970
    """
    (basestr, usecstr) = text.split(".", 1)
    assert usecstr[-1] == "Z"
    tm_usec = int(usecstr[:-1])
    tm = time.strptime(basestr, "%Y-%m-%dT%H:%M:%S")
    return (int(calendar.timegm(tm)) * 1000000 + tm_usec)


def parse_externals_description(base_url, val):
    """Parse an svn:externals property value.

    :param base_url: URL on which the property is set. Used for
        relative externals.

    :returns: dictionary with local names as keys, (revnum, url)
              as value. revnum is the revision number and is
              set to None if not applicable.
    """
    def is_url(u):
        return ("://" in u)
    ret = {}
    for l in val.splitlines():
        if l == "" or l[0] == "#":
            continue
        pts = l.rsplit(None, 3)
        if len(pts) == 4:
            if pts[0] == "-r":  # -r X URL DIR
                revno = int(pts[1])
                path = pts[3]
                relurl = pts[2]
            elif pts[1] == "-r":  # DIR -r X URL
                revno = int(pts[2])
                path = pts[0]
                relurl = pts[3]
            else:
                raise InvalidExternalsDescription()
        elif len(pts) == 3:
            if pts[1].startswith("-r"):  # DIR -rX URL
                revno = int(pts[1][2:])
                path = pts[0]
                relurl = pts[2]
            elif pts[0].startswith("-r"):  # -rX URL DIR
                revno = int(pts[0][2:])
                path = pts[2]
                relurl = pts[1]
            else:
                raise InvalidExternalsDescription()
        elif len(pts) == 2:
            if not is_url(pts[0]):
                relurl = pts[1]
                path = pts[0]
            else:
                relurl = pts[0]
                path = pts[1]
            revno = None
        else:
            raise InvalidExternalsDescription()
        if relurl.startswith("//"):
            raise NotImplementedError(
                "Relative to the scheme externals not yet supported")
        if relurl.startswith("^/"):
            raise NotImplementedError(
                "Relative to the repository root externals not yet supported")
        ret[path] = (revno, urlparse.urljoin(base_url+"/", relurl))
    return ret


def parse_mergeinfo_property(text):
    """Parse a mergeinfo property.

    :param text: Property contents
    """
    ret = {}
    for l in text.splitlines():
        (path, ranges) = l.rsplit(":", 1)
        assert path.startswith("/")
        ret[path] = []
        for range in ranges.split(","):
            if range[-1] == "*":
                inheritable = False
                range = range[:-1]
            else:
                inheritable = True
            try:
                (start, end) = range.split("-", 1)
                ret[path].append((int(start), int(end), inheritable))
            except ValueError:
                ret[path].append((int(range), int(range), inheritable))

    return ret


def generate_mergeinfo_property(merges):
    """Generate the contents of the svn:mergeinfo property

    :param merges: dictionary mapping paths to lists of ranges
    :return: Property contents
    """
    def formatrange(range_params):
        (start, end, inheritable) = range_params
        suffix = ""
        if not inheritable:
            suffix = "*"
        if start == end:
            return "%d%s" % (start, suffix)
        else:
            return "%d-%d%s" % (start, end, suffix)
    text = ""
    for (path, ranges) in merges.items():
        assert path.startswith("/")
        text += "%s:%s\n" % (path, ",".join(map(formatrange, ranges)))
    return text


def range_includes_revnum(ranges, revnum):
    """Check if the specified range contains the mentioned revision number.

    :param ranges: list of ranges
    :param revnum: revision number
    :return: Whether or not the revision number is included
    """
    i = bisect.bisect(ranges, (revnum, revnum, True))
    if i == 0:
        return False
    (start, end, inheritable) = ranges[i-1]
    return (start <= revnum <= end)


def range_add_revnum(ranges, revnum, inheritable=True):
    """Add revision number to a list of ranges

    :param ranges: List of ranges
    :param revnum: Revision number to add
    :param inheritable: TODO
    :return: New list of ranges
    """
    # TODO: Deal with inheritable
    item = (revnum, revnum, inheritable)
    if len(ranges) == 0:
        ranges.append(item)
        return ranges
    i = bisect.bisect(ranges, item)
    if i > 0:
        (start, end, inh) = ranges[i-1]
        if (start <= revnum <= end):
            # already there
            return ranges
        if end == revnum-1:
            # Extend previous range
            ranges[i-1] = (start, end+1, inh)
            return ranges
    if i < len(ranges):
        (start, end, inh) = ranges[i]
        if start-1 == revnum:
            # Extend next range
            ranges[i] = (start-1, end, inh)
            return ranges
    ranges.insert(i, item)
    return ranges


def mergeinfo_includes_revision(merges, path, revnum):
    """Check if the specified mergeinfo contains a path in revnum.

    :param merges: Dictionary with merges
    :param path: Merged path
    :param revnum: Revision number
    :return: Whether the revision is included
    """
    assert path.startswith("/")
    try:
        ranges = merges[path]
    except KeyError:
        return False

    return range_includes_revnum(ranges, revnum)


def mergeinfo_add_revision(mergeinfo, path, revnum):
    """Add a revision to a mergeinfo dictionary

    :param mergeinfo: Merginfo dictionary
    :param path: Merged path to add
    :param revnum: Merged revision to add
    :return: Updated dictionary
    """
    assert path.startswith("/")
    mergeinfo[path] = range_add_revnum(mergeinfo.get(path, []), revnum)
    return mergeinfo


PROP_EXECUTABLE = 'svn:executable'
PROP_EXECUTABLE_VALUE = '*'
PROP_EXTERNALS = 'svn:externals'
PROP_IGNORE = 'svn:ignore'
PROP_KEYWORDS = 'svn:keywords'
PROP_MIME_TYPE = 'svn:mime-type'
PROP_MERGEINFO = 'svn:mergeinfo'
PROP_NEEDS_LOCK = 'svn:needs-lock'
PROP_NEEDS_LOCK_VALUE = '*'
PROP_PREFIX = 'svn:'
PROP_SPECIAL = 'svn:special'
PROP_SPECIAL_VALUE = '*'
PROP_WC_PREFIX = 'svn:wc:'
PROP_ENTRY_PREFIX = 'svn:entry'
PROP_ENTRY_COMMITTED_DATE = 'svn:entry:committed-date'
PROP_ENTRY_COMMITTED_REV = 'svn:entry:committed-rev'
PROP_ENTRY_LAST_AUTHOR = 'svn:entry:last-author'
PROP_ENTRY_LOCK_TOKEN = 'svn:entry:lock-token'
PROP_ENTRY_UUID = 'svn:entry:uuid'

PROP_REVISION_LOG = "svn:log"
PROP_REVISION_AUTHOR = "svn:author"
PROP_REVISION_DATE = "svn:date"
PROP_REVISION_ORIGINAL_DATE = "svn:original-date"


def diff(current, previous):
    """Find the differences between two property dictionaries.

    :param current: Dictionary with current (new) properties
    :param previous: Dictionary with previous (old) properties
    :return: Dictionary that contains an entry for
             each property that was changed. Value is a tuple
             with the old and the new property value.
    """
    ret = {}
    for key, newval in current.items():
        oldval = previous.get(key)
        if oldval != newval:
            ret[key] = (oldval, newval)
    return ret
