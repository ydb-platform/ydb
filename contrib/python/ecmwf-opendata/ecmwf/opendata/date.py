#!/usr/bin/env python
# (C) Copyright 2021 ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation
# nor does it submit to any jurisdiction.
#
import datetime
import re

VALID_DATE = re.compile(r"\d\d\d\d-\d\d-\d\d([T\s]\d\d:\d\d(:\d\d)?)?Z?")


def end_step(p):
    if isinstance(p, str):
        return int(p.split("-")[-1])
    return int(p)


def canonical_time(time):
    if isinstance(time, datetime.time):
        assert datetime.time(time.hour) == time, time
        return time.hour
    time = int(time)
    if time >= 100:
        assert time % 100 == 0, time
        time //= 100
    return time


def full_date(date, time=None):
    if isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
        date = datetime.datetime(date.year, date.month, date.day)

    if isinstance(date, int):
        if date <= 0:
            date = datetime.datetime.utcnow() + datetime.timedelta(days=date)
            date = datetime.datetime(date.year, date.month, date.day)
        else:
            date = datetime.datetime(date // 10000, date % 10000 // 100, date % 100)

    if isinstance(date, str):
        try:
            return full_date(int(date), time)
        except ValueError:
            pass

        if VALID_DATE.match(date):
            date = datetime.datetime.fromisoformat(date)

    if not isinstance(date, datetime.datetime):
        raise ValueError("Invalid date: {} ({})".format(date, type(date)))

    if time is not None:
        time = canonical_time(time)
        date = datetime.datetime(date.year, date.month, date.day, time, 0, 0)

    return date


def _expandable(lst):
    if len(lst) not in (3, 5):
        return False

    if not isinstance(lst[1], str) or lst[1].lower() != "to":
        return False

    if len(lst) == 5 and (not isinstance(lst[3], str) or lst[3].lower() != "by"):
        return False

    return True


def expand_list(lst):
    if not _expandable(lst):
        return lst

    start = int(lst[0])
    end = int(lst[2])
    by = 1

    if len(lst) == 5:
        by = int(lst[4])

    assert start <= end and by > 0, (start, end, by)

    return list(range(start, end + by, by))


def expand_date(lst):
    if not _expandable(lst):
        return lst

    start = full_date(lst[0])
    end = full_date(lst[2])
    by = 1

    if len(lst) == 5:
        by = int(lst[4])

    assert start <= end and by > 0, (start, end, by)

    result = []
    by = datetime.timedelta(days=by)

    while start <= end:
        result.append(start.strftime("%Y%m%d"))
        start += by

    return result


def expand_time(lst):
    if not _expandable(lst):
        return lst

    start = canonical_time(lst[0])
    end = canonical_time(lst[2])
    by = 6

    if len(lst) == 5:
        by = int(lst[4])

    assert start <= end and by > 0, (start, end, by)

    return list(range(start, end + by, by))
