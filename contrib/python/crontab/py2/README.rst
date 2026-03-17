
Copyright 2011-2021 Josiah Carlson

Released under the LGPL license version 2.1 and version 3 (you can choose
which you'd like to be bound under).

Description
===========

This package intends to offer a method of parsing crontab schedule entries and
determining when an item should next be run. More specifically, it calculates
a delay in seconds from when the .next() method is called to when the item
should next be executed.

Comparing the below chart to http://en.wikipedia.org/wiki/Cron#CRON_expression
you will note that W and # symbols are not supported.

============= =========== ================= ============== ===========================
Field Name    Mandatory   Allowed Values    Default Value  Allowed Special Characters
============= =========== ================= ============== ===========================
Seconds       No          0-59              0              \* / , -
Minutes       Yes         0-59              N/A            \* / , -
Hours         Yes         0-23              N/A            \* / , -
Day of month  Yes         1-31              N/A            \* / , - ? L Z
Month         Yes         1-12 or JAN-DEC   N/A            \* / , -
Day of week   Yes         0-6 or SUN-SAT    N/A            \* / , - ? L
Year          No          1970-2099         *              \* / , -
============= =========== ================= ============== ===========================

If your cron entry has 5 values, minutes-day of week are used, default seconds
is and default year is appended. If your cron entry has 6 values, minutes-year
are used, and default seconds are prepended.

As such, only 5-7 value crontab entries are accepted (and mangled to 7 values,
as necessary).


Sample individual crontab fields
================================

Examples of supported entries are as follows::

    *
    */5
    7/8
    3-25/7
    3,7,9
    0-10,30-40/5

For month or day of week entries, 3 letter abbreviations of the month or day
can be used to the left of any optional / where a number could be used.

For days of the week::

    mon-fri
    sun-thu/2

For month::

    apr-jul
    mar-sep/3

Installation
============

::

    pip install crontab


Example uses
============

::

    >>> from crontab import CronTab
    >>> from datetime import datetime
    >>> # define the crontab for 25 minutes past the hour every hour
    ... entry = CronTab('25 * * * *')
    >>> # find the delay from when this was run (around 11:13AM)
    ... entry.next()
    720.81637899999998
    >>> # find the delay from when it was last scheduled
    ... entry.next(datetime(2011, 7, 17, 11, 25))
    3600.0




Notes
=====

At most one of 'day of week' or 'day of month' can be a value other than '?'
or '*'. We violate spec here and allow '*' to be an alias for '?', in the case
where one of those values is specified (seeing as some platforms don't support
'?').

This module also supports the convenient aliases::

    @yearly
    @annually
    @monthly
    @weekly
    @daily
    @hourly

Example full crontab entries and their meanings::

    30 */2 * * * -> 30 minutes past the hour every 2 hours
    15,45 23 * * * -> 11:15PM and 11:45PM every day
    0 1 ? * SUN -> 1AM every Sunday
    0 1 * * SUN -> 1AM every Sunday (same as above)
    0 0 1 jan/2 * 2011-2013 ->
        midnight on January 1, 2011 and the first of every odd month until
        the end of 2013
    24 7 L * * -> 7:24 AM on the last day of every month
    24 7 * * L5 -> 7:24 AM on the last friday of every month
    24 7 * * Lwed-fri ->
        7:24 AM on the last wednesday, thursday, and friday of every month
    0 8 L * * -> 8 AM on the last day of the month, every month
    0 8 Z0 * * -> 8 AM on the last day of the month, z0 is an alias for L
    0 8 Z1 * * -> 8 AM 1 day before the last day of the month, every month
    0 8 Z2 * * -> 8 AM 2 days before last day of the month, every month

