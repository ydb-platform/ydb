# A Chinese Calendar Library in Pure Python

[![Python package](https://github.com/lidaobing/python-lunardate/actions/workflows/python-package.yml/badge.svg?branch=master)](https://github.com/lidaobing/python-lunardate/actions/workflows/python-package.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/lunardate)](https://pypi.org/project/lunardate/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/lunardate)](https://pypistats.org/packages/lunardate)


Chinese Calendar: http://en.wikipedia.org/wiki/Chinese_calendar

## Install

```
pip install lunardate
```

## Usage

```
        >>> from lunardate import LunarDate
        >>> LunarDate.fromSolarDate(1976, 10, 1)
        LunarDate(1976, 8, 8, 1)
        >>> LunarDate(1976, 8, 8, 1).toSolarDate()
        datetime.date(1976, 10, 1)
        >>> LunarDate(1976, 8, 8, 1).year
        1976
        >>> LunarDate(1976, 8, 8, 1).month
        8
        >>> LunarDate(1976, 8, 8, 1).day
        8
        >>> LunarDate(1976, 8, 8, 1).isLeapMonth
        True

        >>> today = LunarDate.today()
        >>> type(today).__name__
        'LunarDate'

        >>> # support '+' and '-' between datetime.date and datetime.timedelta
        >>> ld = LunarDate(1976,8,8)
        >>> sd = datetime.date(2008,1,1)
        >>> td = datetime.timedelta(days=10)
        >>> ld-ld
        datetime.timedelta(0)
        >>> ld-sd
        datetime.timedelta(-11444)
        >>> ld-td
        LunarDate(1976, 7, 27, 0)
        >>> sd-ld
        datetime.timedelta(11444)
        >>> ld+td
        LunarDate(1976, 8, 18, 0)
        >>> td+ld
        LunarDate(1976, 8, 18, 0)
        >>> ld2 = LunarDate.today()
        >>> ld < ld2
        True
        >>> ld <= ld2
        True
        >>> ld > ld2
        False
        >>> ld >= ld2
        False
        >>> ld == ld2
        False
        >>> ld != ld2
        True
        >>> ld == ld
        True
        >>> LunarDate.today() == LunarDate.today()
        True

        >>> LunarDate.leapMonthForYear(2023)
        2
        >>> LunarDate.leapMonthForYear(2022)
        None
```

## News

* 0.2.2: add LunarDate.leapMonthForYear; fix bug in year 1899
* 0.2.1: fix bug in year 1956
* 0.2.0: extend year to 2099, thanks to @FuGangqiang
* 0.1.5: fix bug in `==`
* 0.1.4: support '+', '-' and compare, fix bug in year 2050
* 0.1.3: support python 3.0

## Limits

this library can only deal with year from 1900 to 2099 (in chinese calendar).

## See also

* lunar: http://packages.qa.debian.org/l/lunar.html,
  A converter written in C, this program is derived from it.
* python-lunar: http://code.google.com/p/liblunar/
  Another library written in C, including a python binding.
