# udatetime: Fast RFC3339 compliant date-time library

Handling date-times is a painful act because of the sheer endless amount
of formats used by people. Luckily there are a couple of specified standards
out there like [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). But even
ISO 8601 leaves to many options on how to define date and time. That's why I
encourage using the [RFC3339](https://www.ietf.org/rfc/rfc3339.txt) specified
date-time format.

`udatetime` offers on average 76% faster `datetime` object instantiation,
serialization and deserialization of RFC3339 date-time strings. `udatetime` is
using Python's [datetime class](https://docs.python.org/2/library/datetime.html)
under the hood and code already using `datetime` should be able to easily
switch to `udatetime`. All `datetime` objects created by `udatetime` are
timezone-aware. The timezones that `udatetime` uses are fixed-offset timezones,
meaning that they don't observe daylight savings time (DST), and thus return a
fixed offset from UTC all year round.

|          | Support            | Performance optimized | Implementation |
| -------- |:------------------:|:---------------------:| -------------- |
| Python 2 | :heavy_check_mark: |  :heavy_check_mark:   | C              |
| Python 3 | :heavy_check_mark: |  :heavy_check_mark:   | C              |
| PyPy     | :heavy_check_mark: |  :heavy_check_mark:   | Pure Python    |

```python
>>> udatetime.from_string("2016-07-15T12:33:20.123000+02:00")
datetime.datetime(2016, 7, 15, 12, 33, 20, 123000, tzinfo=+02:00)

>>> dt = udatetime.from_string("2016-07-15T12:33:20.123000+02:00")
>>> udatetime.to_string(dt)
'2016-07-15T12:33:20.123000+02:00'

>>> udatetime.now()
datetime.datetime(2016, 7, 29, 10, 15, 24, 472467, tzinfo=+02:00)

>>> udatetime.utcnow()
datetime.datetime(2016, 7, 29, 8, 15, 36, 184762, tzinfo=+00:00)

>>> udatetime.now_to_string()
'2016-07-29T10:15:46.641233+02:00'

>>> udatetime.utcnow_to_string()
'2016-07-29T08:15:56.129798+00:00'

>>> udatetime.to_string(udatetime.utcnow() - timedelta(hours=6))
'2016-07-29T02:16:05.770358+00:00'

>>> udatetime.fromtimestamp(time.time())
datetime.datetime(2016, 7, 30, 17, 45, 1, 536586, tzinfo=+02:00)

>>> udatetime.utcfromtimestamp(time.time())
datetime.datetime(2016, 8, 1, 10, 14, 53, tzinfo=+00:00)
```

## Installation

Currently only **POSIX** compliant systems are supported.
Working on cross-platform support.

```
$ pip install udatetime
```

You might need to install the header files of your Python installation and
some essential tools to execute the build like a C compiler.

**Python 2**

```
$ sudo apt-get install python-dev build-essential
```

or

```
$ sudo yum install python-devel gcc
```

**Python 3**

```
$ sudo apt-get install python3-dev build-essential
```

or

```
$ sudo yum install python3-devel gcc
```

## Benchmark

The benchmarks compare the performance of equivalent code of `datetime` and
`udatetime`. The benchmark measures the time needed for 1 million executions
and takes the `min` of 3 repeats. You can run the benchmark yourself and see
the results on your machine by executing the `bench_udatetime.py` script.

![Benchmark interpreter summary](/extras/benchmark_interpreter_summary.png?raw=true "datetime vs. udatetime summary")

### Python 2.7

```
$ python scripts/bench_udatetime.py
Executing benchmarks ...

============ benchmark_parse
datetime_strptime 10.6306970119
udatetime_parse 1.109801054
udatetime is 9.6 times faster

============ benchmark_format
datetime_strftime 2.08363199234
udatetime_format 0.654432058334
udatetime is 3.2 times faster

============ benchmark_utcnow
datetime_utcnow 0.485884904861
udatetime_utcnow 0.237742185593
udatetime is 2.0 times faster

============ benchmark_now
datetime_now 1.37059998512
udatetime_now 0.235424041748
udatetime is 5.8 times faster

============ benchmark_utcnow_to_string
datetime_utcnow_to_string 2.56599593163
udatetime_utcnow_to_string 0.685483932495
udatetime is 3.7 times faster

============ benchmark_now_to_string
datetime_now_to_string 3.68989396095
udatetime_now_to_string 0.687911987305
udatetime is 5.4 times faster

============ benchmark_fromtimestamp
datetime_fromtimestamp 1.38640713692
udatetime_fromtimestamp 0.287910938263
udatetime is 4.8 times faster

============ benchmark_utcfromtimestamp
datetime_utcfromtimestamp 0.533131837845
udatetime_utcfromtimestamp 0.279694080353
udatetime is 1.9 times faster
```

### Python 3.5

```
$ python scripts/bench_udatetime.py
Executing benchmarks ...

============ benchmark_parse
datetime_strptime 9.90670353400003
udatetime_parse 1.1668808249999074
udatetime is 8.5 times faster

============ benchmark_format
datetime_strftime 3.0286041580000074
udatetime_format 0.7153575119999687
udatetime is 4.2 times faster

============ benchmark_utcnow
datetime_utcnow 0.5638177430000724
udatetime_utcnow 0.2548112540000602
udatetime is 2.2 times faster

============ benchmark_now
datetime_now 1.457822759999999
udatetime_now 0.26195338699994863
udatetime is 5.6 times faster

============ benchmark_utcnow_to_string
datetime_utcnow_to_string 3.5347913849999486
udatetime_utcnow_to_string 0.750341161999927
udatetime is 4.7 times faster

============ benchmark_now_to_string
datetime_now_to_string 4.854975383999999
udatetime_now_to_string 0.7411948169999505
udatetime is 6.6 times faster

============ benchmark_fromtimestamp
datetime_fromtimestamp 1.4233373309999706
udatetime_fromtimestamp 0.31758270299997093
udatetime is 4.5 times faster

============ benchmark_utcfromtimestamp
datetime_utcfromtimestamp 0.5633522409999614
udatetime_utcfromtimestamp 0.305099536000057
udatetime is 1.8 times faster
```

### PyPy 5.3.1

```
$ python scripts/bench_udatetime.py
Executing benchmarks ...

============ benchmark_parse
datetime_strptime 2.31050491333
udatetime_parse 0.838973045349
udatetime is 2.8 times faster

============ benchmark_format
datetime_strftime 0.957178115845
udatetime_format 0.162060976028
udatetime is 5.9 times faster

============ benchmark_utcnow
datetime_utcnow 0.149839878082
udatetime_utcnow 0.149217844009
udatetime is as fast as datetime

============ benchmark_now
datetime_now 0.967023134232
udatetime_now 0.15003991127
udatetime is 6.4 times faster

============ benchmark_utcnow_to_string
datetime_utcnow_to_string 1.24988698959
udatetime_utcnow_to_string 0.632546901703
udatetime is 2.0 times faster

============ benchmark_now_to_string
datetime_now_to_string 2.13041496277
udatetime_now_to_string 0.607964038849
udatetime is 3.5 times faster

============ benchmark_fromtimestamp
datetime_fromtimestamp 0.903736114502
udatetime_fromtimestamp 0.0907990932465
udatetime is 10.0 times faster

============ benchmark_utcfromtimestamp
datetime_utcfromtimestamp 0.0890419483185
udatetime_utcfromtimestamp 0.0907027721405
udatetime is as fast as datetime
```

## Why RFC3339

The RFC3339 specification has the following advantages:

- Defined date, time, timezone, date-time format
- 4 digit year
- Fractional seconds
- Human readable
- No redundant information like weekday name
- Simple specification, easily machine readable

### RFC3339 format specification

```
date-fullyear   = 4DIGIT
date-month      = 2DIGIT  ; 01-12
date-mday       = 2DIGIT  ; 01-28, 01-29, 01-30, 01-31 based on
                          ; month/year
time-hour       = 2DIGIT  ; 00-23
time-minute     = 2DIGIT  ; 00-59
time-second     = 2DIGIT  ; 00-58, 00-59, 00-60 based on leap second
                          ; rules
time-secfrac    = "." 1*DIGIT
time-numoffset  = ("+" / "-") time-hour ":" time-minute
time-offset     = "Z" / time-numoffset

partial-time    = time-hour ":" time-minute ":" time-second [time-secfrac]

full-date       = date-fullyear "-" date-month "-" date-mday
full-time       = partial-time time-offset

date-time       = full-date "T" full-time
```

`udatetime` specific format addons:

- time-secfrac from 1DIGIT up to 6DIGIT
- time-secfrac will be normalized to microseconds
- time-offset is optional. Missing time-offset will be treated as UTC.
- spaces will be eliminated

## Why in C?

The Python `datetime` library is flexible but painfully slow, when it comes to
parsing and formating. High performance applications like web services or
logging benefit from fast underlying libraries. Restricting the input format
to one standard allows for optimization and results in speed improvements.
