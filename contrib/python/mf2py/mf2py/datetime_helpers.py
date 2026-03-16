"""helper functions to deal wit datetime strings"""

import re
from datetime import datetime

# REGEX!

DATE_RE = r"(\d{4}-\d{2}-\d{2})|(\d{4}-\d{3})"
SEC_RE = r"(:(?P<second>\d{2})(\.\d+)?)"
RAWTIME_RE = r"(?P<hour>\d{1,2})(:(?P<minute>\d{2})%s?)?" % (SEC_RE)
AMPM_RE = r"am|pm|a\.m\.|p\.m\.|AM|PM|A\.M\.|P\.M\."
TIMEZONE_RE = r"Z|[+-]\d{1,2}:?\d{2}?"
TIME_RE = r"(?P<rawtime>%s)( ?(?P<ampm>%s))?( ?(?P<tz>%s))?" % (
    RAWTIME_RE,
    AMPM_RE,
    TIMEZONE_RE,
)
DATETIME_RE = r"(?P<date>%s)(?P<separator>[T ])(?P<time>%s)" % (DATE_RE, TIME_RE)


def normalize_datetime(dtstr, match=None):
    """Try to normalize a datetime string.
    1. Convert 12-hour time to 24-hour time

    pass match in if we have already calculated it to avoid rework
    """
    match = match or (dtstr and re.match(DATETIME_RE + "$", dtstr))
    if match:
        datestr = match.group("date")
        hourstr = match.group("hour")
        minutestr = match.group("minute") or "00"
        secondstr = match.group("second")
        ampmstr = match.group("ampm")
        separator = match.group("separator")

        # convert ordinal date YYYY-DDD to YYYY-MM-DD
        try:
            datestr = datetime.strptime(datestr, "%Y-%j").strftime("%Y-%m-%d")
        except ValueError:
            # datestr was not in YYYY-DDD format
            pass

        # 12 to 24 time conversion
        if ampmstr:
            hourstr = match.group("hour")
            hourint = int(hourstr)

            if (ampmstr.startswith("a") or ampmstr.startswith("A")) and hourint == 12:
                hourstr = "00"

            if (ampmstr.startswith("p") or ampmstr.startswith("P")) and hourint < 12:
                hourstr = hourint + 12

        dtstr = "%s%s%s:%s" % (datestr, separator, hourstr, minutestr)

        if secondstr:
            dtstr += ":" + secondstr

        tzstr = match.group("tz")
        if tzstr:
            dtstr += tzstr.replace(":", "")
    return dtstr
