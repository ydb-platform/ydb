#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import calendar
from datetime import datetime
import os


def isoformat(utc_date):
    return datetime.isoformat(utc_date).replace('T', ' ')


def get_file_mtime(location, iso=True):
    """
    Return a string containing the last modified date of a file formatted
    as an ISO time stamp if ISO is True or as a raw number since epoch.
    """
    date = ''
    # FIXME: use file types
    if not os.path.isdir(location):
        mtime = os.stat(location).st_mtime
        if iso:
            utc_date = datetime.utcfromtimestamp(mtime)
            date = isoformat(utc_date)
        else:
            date = str(mtime)
    return date


def secs_from_epoch(d):
    """
    Return a number of seconds since epoch for a date time stamp
    """
    # FIXME: what does this do?
    return calendar.timegm(datetime.strptime(d.split('.')[0],
                                    '%Y-%m-%d %H:%M:%S').timetuple())
