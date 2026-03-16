convertdate
===========

The convertdate package was originally developed as "[Python Date
Utils](http://sourceforge.net/projects/pythondateutil/)" by Phil
Schwartz. It had been significantly updated and expanded.

Available calendars:

-   Armenian
-   Bahai
-   Coptic (Alexandrian)
-   French Republican
-   Gregorian
-   Hebrew
-   Indian Civil
-   Islamic
-   Julian
-   Mayan
-   Persian
-   Positivist
-   Mayan
-   ISO
-   Ordinal (day of year)
-   Dublin day count
-   Julian day count

The `holidays` module also provides some useful holiday-calculation,
with a focus on North American and Jewish holidays.

Installing
----------

`pip install convertdate`

Or download the package and run `python setup.py install`.

Using
-----

    from convertdate import french_republican
    from convertdate import hebrew

    french_republican.from_gregorian(2014, 10, 31)
    # (223, 2, 1, 9)

    hebrew.from_gregorian(2014, 10, 31)
    # (5775, 8, 7)

Note that in some calendar systems, the day begins at sundown.
Convertdate gives the conversion for noon of the day in question.

Each module includes a monthcalendar function, which will generate a
calender-like nested list for a year and month (each list of dates runs
from Sunday to Saturday)

    hebrew.monthcalendar(5775, 8)
    # [
    #     [None, None, None, None, None, None, 1],
    #     [2, 3, 4, 5, 6, 7, 8],
    #     [9, 10, 11, 12, 13, 14, 15],
    #     [16, 17, 18, 19, 20, 21, 22],
    #     [23, 24, 25, 26, 27, 28, 29]
    # ]

    julian.monthcalendar(2015, 1)
    # [
    #    [None, None, None, 1, 2, 3, 4],
    #    [5, 6, 7, 8, 9, 10, 11],
    #    [12, 13, 14, 15, 16, 17, 18],
    #    [19, 20, 21, 22, 23, 24, 25],
    #    [26, 27, 28, 29, 30, 31, None]
    # ]

Special Options
---------------

### Armenian

The Armenian calendar begins on 11 July 552 (Julian) and has two modes of
reckoning. The first is the invariant-length version consisting of 12 months
of 30 days each and five epagomenal days; the second is the version
established by Yovhannes Sarkawag in 1084, which fixed the first day of the
year with respect to the Julian calendar and added a sixth epagomenal day
every four years.

By default the invariant calendar is used, but the Sarkawag calendar can be
used beginning with the Armenian year 533 (11 August 1084) by passing the
parameter `method='sarkawag'` to the relevant functions.


### French Republican

Leap year calculations in the French Republican calendar are a matter of
dispute. By default, `convertdate` calculates leap years using the
autumnal equinox. You can also use one of three more systematic methods
proposed over the years.

-   Romme, a co-creator of the calendar, proposed leap years in years
    divisible by four, except for years divisible by 100.
-   Some concordances were drawn up in the 19th century that gave leap
    years every 4 years, in years that give a remainder of three when
    divided by four (19, 23, 27, etc...).
-   Von M&auml;dler proposed leap years in years divisible by four, except
    for years divisible by 128.

You can specify any of these three methods with the method keyword
argument in `french_republican` conversion functions.

    from convertdate import french_republican

    # Romme's method
    french_republican.to_gregorian(20, 1, 1), method='romme')
    # (1811, 9, 23)

    # continuous method
    french_republican.to_gregorian(20, 1, 1), method='continuous')
    # (1811, 9, 24)

    # von Madler's method
    french_republican.to_gregorian(20, 1, 1), method='madler')
    # (1811, 9, 23)

All the conversion methods correctly assign the leap years implemented
while calendar was in use (3, 7, 11).

Baha'i
------

The Bah&aacute;'&iacute; (Bad&iacute;) calendar has an intercalary period, Ayyam-i-H&aacute;, which occurs between the 18th and 19th months.
Dates in this period are returned as month 19, and the month of &lsquo;Al&aacute; is reported as month 20.

```python
from convertdate import bahai
# the first day of Ayyam-i-Ha:
bahai.to_gregorian(175, 19, 1)
# (2019, 2, 11)
# The first day of 'Ala:
bahai.to_gregorian(175, 20, 1)
# (2019, 3, 2)
```

Before the Common Era
---------------------

For dates before the Common Era (year 1), `convertdate` uses
astronomical notation: 1 BC is recorded as 0, 2 BC is -1, etc. This
makes arithmatic much easier at the expense of ignoring custom.

Note that for dates before 4 CE, `convertdate` uses the [proleptic
Julian
calendar](https://en.wikipedia.org/wiki/Proleptic_Julian_calendar). The
Julian Calendar was in use from 45 BC, but before 4 CE the leap year
leap year pattern was irregular.

The [proleptic Gregorian
calendar](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar) is
used for dates before 1582 CE, the year of the Gregorian calendar
reform.

Holidays
--------

North American holidays are the current focus of the `holidays` module,
but pull requests are welcome.

    from convertdate import holidays

    # For simplicity, functions in the holidays module return a tuple
    # In the format (year, month, day)

    holidays.new_years(2014)
    # (2014, 1, 1)

    holidays.memorial_day(2014)
    # (2014, 5, 26)

    # USA is default
    holidays.thanksgiving(2014)
    # (2014, 11, 27)

    # But there is a Canadian option for some holidays
    holidays.thanksgiving(2014, 'canada')
    # (2014, 10, 13)

    # Mexican national holidays
    holidays.natalicio_benito_juarez(2016)
    # (2016, 3, 21)

    holidays.dia_revolucion(2016)
    # (2016, 11, 21)

    # Some Jewish holidays are included
    holidays.rosh_hashanah(2014)
    
    # Easter can be calculated according to different churches 
    # ('western', 'orthodox', 'eastern')
    # The eastern Christian computation differs from the Orthodox one
    # 4 times in each 532-year cycle.
    
    holidays.easter(2019)
    # (2019, 4, 21)
    holidays.easter(2019, church="orthodox")
    # (2019, 4, 28)
    holidays.easter(2019, church="orthodox")
    # (2019, 4, 28)

Utils
-----

Convertdate includes some utilities for manipulating and calculating
dates.

    from convertdate import utils

    # Calculate an arbitrary day of the week
    THUR = 3
    APRIL = 4

    # 3rd Thursday in April
    utils.nth_day_of_month(3, THUR, APRIL, 2014)
    # (2014, 4, 17)

    utils.nth_day_of_month(5, THUR, APRIL, 2014)
    # IndexError: No 5th day of month 4

    # Use 0 for the first argument to get the last weekday of a month
    utils.nth_day_of_month(0, THUR, APRIL, 2014)
    # (2014, 4, 24)

Note that when calculating weekdays, convertdate uses the convention of
the calendar and time modules: Monday is 0, Sunday is 6.

    from convertdate import gregorian

    SUN = 6

    day = gregorian.to_jd(2014, 4, 17)
    nextsunday = utils.next_weekday(SUN, day)

    gregorian.from_jd(nextsunday)
    # (2014, 4, 20)

Other utility functions:

-   nearest\_weekday
-   next\_or\_current\_weekday
-   previous\_weekday
-   previous\_or\_current\_weekday

