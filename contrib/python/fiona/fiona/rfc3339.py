# Fiona's date and time is founded on RFC 3339.
#
# OGR knows 3 time "zones": GMT, "local time", amd "unknown". Fiona, when
# writing will convert times with a timezone offset to GMT (Z) and otherwise
# will write times with the unknown zone.

import logging
import re

log = logging.getLogger("Fiona")

pattern_date = re.compile(r"(\d\d\d\d)(-)?(\d\d)(-)?(\d\d)")
pattern_time = re.compile(
    r"(\d\d)(:)?(\d\d)(:)?(\d\d)?(\.\d+)?(Z|([+-])?(\d\d)?(:)?(\d\d))?")
pattern_datetime = re.compile(
    r"(\d\d\d\d)(-)?(\d\d)(-)?(\d\d)(T)?(\d\d)(:)?(\d\d)(:)?(\d\d)?(\.\d+)?(Z|([+-])?(\d\d)?(:)?(\d\d))?")


class group_accessor:
    def __init__(self, m):
        self.match = m

    def group(self, i):
        try:
            return self.match.group(i) or 0
        except IndexError:
            return 0


def parse_time(text):
    """ Given a time, returns a datetime tuple

    Parameters
    ----------
    text: string to be parsed

    Returns
    -------
        (int, int , int, int, int, int, int, int):
            datetime tuple: (year, month, day, hour, minute, second, microsecond, utcoffset in minutes or None)

    """
    match = re.search(pattern_time, text)
    if match is None:
        raise ValueError(f"Time data '{text}' does not match pattern")
    g = group_accessor(match)
    log.debug("Match groups: %s", match.groups())

    if g.group(8) == '-':
        tz = -1.0 * (int(g.group(9)) * 60 + int(g.group(11)))
    elif g.group(8) == '+':
        tz = int(g.group(9)) * 60 + int(g.group(11))
    else:
        tz = None

    return (0, 0, 0,
            int(g.group(1)),
            int(g.group(3)),
            int(g.group(5)),
            int(1000000.0 * float(g.group(6))),
            tz
            )


def parse_date(text):
    """Given a date, returns a datetime tuple

    Parameters
    ----------
    text: string to be parsed

    Returns
    -------
        (int, int , int, int, int, int, int, int):
            datetime tuple: (year, month, day, hour, minute, second, microsecond, utcoffset in minutes or None)
    """
    match = re.search(pattern_date, text)
    if match is None:
        raise ValueError(f"Time data '{text}' does not match pattern")
    g = group_accessor(match)
    log.debug("Match groups: %s", match.groups())
    return (
        int(g.group(1)),
        int(g.group(3)),
        int(g.group(5)),
        0, 0, 0, 0, None)


def parse_datetime(text):
    """Given a datetime, returns a datetime tuple

    Parameters
    ----------
    text: string to be parsed

    Returns
    -------
        (int, int , int, int, int, int, int, int):
            datetime tuple: (year, month, day, hour, minute, second, microsecond, utcoffset in minutes or None)
    """
    match = re.search(pattern_datetime, text)
    if match is None:
        raise ValueError(f"Time data '{text}' does not match pattern")
    g = group_accessor(match)
    log.debug("Match groups: %s", match.groups())

    if g.group(14) == '-':
        tz = -1.0 * (int(g.group(15)) * 60 + int(g.group(17)))
    elif g.group(14) == '+':
        tz = int(g.group(15)) * 60 + int(g.group(17))
    else:
        tz = None

    return (
        int(g.group(1)),
        int(g.group(3)),
        int(g.group(5)),
        int(g.group(7)),
        int(g.group(9)),
        int(g.group(11)),
        int(1000000.0 * float(g.group(12))),
        tz)
