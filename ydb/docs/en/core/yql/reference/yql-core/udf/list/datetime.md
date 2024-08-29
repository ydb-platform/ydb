# DateTime

In the DateTime module, the main internal representation format is `Resource<TM>`, which stores the following date components:

* Year (12 bits).
* Month (4 bits).
* Day (5 bits).
* Hour (5 bits).
* Minute (6 bits).
* Second (6 bits).
* Microsecond (20 bits).
* TimezoneId (16 bits).
* DayOfYear (9 bits): Day since the beginning of the year.
* WeekOfYear (6 bits): Week since the beginning of the year, January 1 is always in week 1.
* WeekOfYearIso8601 (6 bits): Week of the year according to ISO 8601 (the first week is the one that includes January 4).
* DayOfWeek (3 bits): Day of the week.

If the timezone is not GMT, the components store the local time for the relevant timezone.

## Split {#split}

Conversion from a primitive type to an internal representation. It's always successful on a non-empty input.

**List of functions**

* ```DateTime::Split(Date{Flags:AutoMap}) -> Resource<TM>```
* ```DateTime::Split(Datetime{Flags:AutoMap}) -> Resource<TM>```
* ```DateTime::Split(Timestamp{Flags:AutoMap}) -> Resource<TM>```
* ```DateTime::Split(TzDate{Flags:AutoMap}) -> Resource<TM>```
* ```DateTime::Split(TzDatetime{Flags:AutoMap}) -> Resource<TM>```
* ```DateTime::Split(TzTimestamp{Flags:AutoMap}) -> Resource<TM>```

Functions that accept `Resource<TM>` as input, can be called directly from the primitive date/time type. An implicit conversion will be made in this case by calling a relevant `Split` function.

## Make... {#make}

Making a primitive type from an internal representation. It's always successful on a non-empty input.

**List of functions**

* ```DateTime::MakeDate(Resource<TM>{Flags:AutoMap}) -> Date```
* ```DateTime::MakeDatetime(Resource<TM>{Flags:AutoMap}) -> Datetime```
* ```DateTime::MakeTimestamp(Resource<TM>{Flags:AutoMap}) -> Timestamp```
* ```DateTime::MakeTzDate(Resource<TM>{Flags:AutoMap}) -> TzDate```
* ```DateTime::MakeTzDatetime(Resource<TM>{Flags:AutoMap}) -> TzDatetime```
* ```DateTime::MakeTzTimestamp(Resource<TM>{Flags:AutoMap}) -> TzTimestamp```

**Examples**

```yql
SELECT
    DateTime::MakeTimestamp(DateTime::Split(Datetime("2019-01-01T15:30:00Z"))),
      -- 2019-01-01T15:30:00.000000Z
    DateTime::MakeDate(Datetime("2019-01-01T15:30:00Z")),
      -- 2019-01-01
    DateTime::MakeTimestamp(DateTime::Split(TzDatetime("2019-01-01T00:00:00,Europe/Moscow"))),
      -- 2018-12-31T21:00:00Z (conversion to UTC)
    DateTime::MakeDate(TzDatetime("2019-01-01T12:00:00,GMT"))
      -- 2019-01-01 (Datetime -> Date with implicit Split)>
```

## Get... {#get}

Extracting a component from an internal representation.

**List of functions**

* ```DateTime::GetYear(Resource<TM>{Flags:AutoMap}) -> Uint16```
* ```DateTime::GetDayOfYear(Resource<TM>{Flags:AutoMap}) -> Uint16```
* ```DateTime::GetMonth(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetMonthName(Resource<TM>{Flags:AutoMap}) -> String```
* ```DateTime::GetWeekOfYear(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetWeekOfYearIso8601(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetDayOfMonth(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetDayOfWeek(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetDayOfWeekName(Resource<TM>{Flags:AutoMap}) -> String```
* ```DateTime::GetHour(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetMinute(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetSecond(Resource<TM>{Flags:AutoMap}) -> Uint8```
* ```DateTime::GetMillisecondOfSecond(Resource<TM>{Flags:AutoMap}) -> Uint32```
* ```DateTime::GetMicrosecondOfSecond(Resource<TM>{Flags:AutoMap}) -> Uint32```
* ```DateTime::GetTimezoneId(Resource<TM>{Flags:AutoMap}) -> Uint16```
* ```DateTime::GetTimezoneName(Resource<TM>{Flags:AutoMap}) -> String```

**Examples**

```yql
$tm = DateTime::Split(TzDatetime("2019-01-09T00:00:00,Europe/Moscow"));

SELECT
    DateTime::GetDayOfMonth($tm) as Day, -- 9
    DateTime::GetMonthName($tm) as Month, -- "January"
    DateTime::GetYear($tm) as Year, -- 2019
    DateTime::GetTimezoneName($tm) as TzName, -- "Europe/Moscow"
    DateTime::GetDayOfWeekName($tm) as WeekDay; -- "Wednesday"
```

## Update {#update}

Updating one or more components in the internal representation. Returns either an updated copy or NULL, if an update produces an invalid date or other inconsistencies.

**List of functions**

* ```DateTime::Update( Resource<TM>{Flags:AutoMap}, [ Year:Uint16?, Month:Uint8?, Day:Uint8?, Hour:Uint8?, Minute:Uint8?, Second:Uint8?, Microsecond:Uint32?, Timezone:String? ]) -> Resource<TM>?```

**Examples**

```yql
$tm = DateTime::Split(Timestamp("2019-01-01T01:02:03.456789Z"));

SELECT
    DateTime::MakeDate(DateTime::Update($tm, 2012)), -- 2012-01-01
    DateTime::MakeDate(DateTime::Update($tm, 2000, 6, 6)), -- 2000-06-06
    DateTime::MakeDate(DateTime::Update($tm, NULL, 2, 30)), -- NULL (February 30)
    DateTime::MakeDatetime(DateTime::Update($tm, NULL, NULL, 31)), -- 2019-01-31T01:02:03Z
    DateTime::MakeDatetime(DateTime::Update($tm, 15 as Hour, 30 as Minute)), -- 2019-01-01T15:30:03Z
    DateTime::MakeTimestamp(DateTime::Update($tm, 999999 as Microsecond)), -- 2019-01-01T01:02:03.999999Z
    DateTime::MakeTimestamp(DateTime::Update($tm, "Europe/Moscow" as Timezone)), -- 2018-12-31T22:02:03.456789Z (conversion to UTC)
    DateTime::MakeTzTimestamp(DateTime::Update($tm, "Europe/Moscow" as Timezone)); -- 2019-01-01T01:02:03.456789,Europe/Moscow
```

## From... {#from}

Getting a Timestamp from the number of seconds/milliseconds/microseconds since the UTC epoch. When the Timestamp limits are exceeded, NULL is returned.

**List of functions**

* ```DateTime::FromSeconds(Uint32{Flags:AutoMap}) -> Timestamp```
* ```DateTime::FromMilliseconds(Uint64{Flags:AutoMap}) -> Timestamp```
* ```DateTime::FromMicroseconds(Uint64{Flags:AutoMap}) -> Timestamp```

## To... {#to}

Getting a number of seconds/milliseconds/microseconds since the UTC Epoch from a primitive type.

**List of functions**

* ```DateTime::ToSeconds(Date/DateTime/Timestamp/TzDate/TzDatetime/TzTimestamp{Flags:AutoMap}) -> Uint32```
* ```DateTime::ToMilliseconds(Date/DateTime/Timestamp/TzDate/TzDatetime/TzTimestamp{Flags:AutoMap}) -> Uint64```
* ```DateTime::ToMicroseconds(Date/DateTime/Timestamp/TzDate/TzDatetime/TzTimestamp{Flags:AutoMap}) -> Uint64```

**Examples**

```yql
SELECT
    DateTime::FromSeconds(1546304523), -- 2019-01-01T01:02:03.000000Z
    DateTime::ToMicroseconds(Timestamp("2019-01-01T01:02:03.456789Z")); -- 1546304523456789
```

## Interval... {#interval}

Conversions between ```Interval``` and various time units.

**List of functions**

* ```DateTime::ToDays(Interval{Flags:AutoMap}) -> Int16```
* ```DateTime::ToHours(Interval{Flags:AutoMap}) -> Int32```
* ```DateTime::ToMinutes(Interval{Flags:AutoMap}) -> Int32```
* ```DateTime::ToSeconds(Interval{Flags:AutoMap}) -> Int32```
* ```DateTime::ToMilliseconds(Interval{Flags:AutoMap}) -> Int64```
* ```DateTime::ToMicroseconds(Interval{Flags:AutoMap}) -> Int64```
* ```DateTime::IntervalFromDays(Int16{Flags:AutoMap}) -> Interval```
* ```DateTime::IntervalFromHours(Int32{Flags:AutoMap}) -> Interval```
* ```DateTime::IntervalFromMinutes(Int32{Flags:AutoMap}) -> Interval```
* ```DateTime::IntervalFromSeconds(Int32{Flags:AutoMap}) -> Interval```
* ```DateTime::IntervalFromMilliseconds(Int64{Flags:AutoMap}) -> Interval```
* ```DateTime::IntervalFromMicroseconds(Int64{Flags:AutoMap}) -> Interval```

AddTimezone doesn't affect the output of ToSeconds() in any way, because ToSeconds() always returns GMT time.

You can also create an Interval from a string literal in the format [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601%23Durations).

**Examples**

```yql
SELECT
    DateTime::ToDays(Interval("PT3000M")), -- 2
    DateTime::IntervalFromSeconds(1000000), -- 11 days 13 hours 46 minutes 40 seconds
    DateTime::ToDays(cast('2018-01-01' as date) - cast('2017-12-31' as date)); --1
```

## StartOf... / TimeOfDay {#startof}

Get the start of the period including the date/time. If the result is invalid, NULL is returned. If the timezone is different from GMT, then the period start is in the specified time zone.

**List of functions**

* ```DateTime::StartOfYear(Resource<TM>{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::StartOfQuarter(Resource<TM>{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::StartOfMonth(Resource<TM>{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::StartOfWeek(Resource<TM>{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::StartOfDay(Resource<TM>{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::StartOf(Resource<TM>{Flags:AutoMap}, Interval{Flags:AutoMap}) -> Resource<TM>?```

The `StartOf` function is intended for grouping by an arbitrary period within a day. The result differs from the input value only by time components. A period exceeding one day is treated as a day (an equivalent of `StartOfDay`). If a day doesn't include an integer number of periods, the number is rounded to the nearest time from the beginning of the day that is a multiple of the specified period. When the interval is zero, the output is same as the input. A negative interval is treated as a positive one.

The functions treat periods longer than one day in a different manner than the same-name functions in the old library. The time components are always reset to zero (this makes sense, because these functions are mainly used for grouping by the period). You can also specify a time period within a day:

* ```DateTime::TimeOfDay(Resource<TM>{Flags:AutoMap}) -> Interval```

**Examples**

```yql
SELECT
    DateTime::MakeDate(DateTime::StartOfYear(Date("2019-06-06"))),
      -- 2019-01-01 (implicit Split here and below)
    DateTime::MakeDatetime(DateTime::StartOfQuarter(Datetime("2019-06-06T01:02:03Z"))),
      -- 2019-04-01T00:00:00Z (time components are reset to zero)
    DateTime::MakeDate(DateTime::StartOfMonth(Timestamp("2019-06-06T01:02:03.456789Z"))),
      -- 2019-06-01
    DateTime::MakeDate(DateTime::StartOfWeek(Date("1970-01-01"))),
      -- NULL (the beginning of the epoch is Thursday, the beginning of the week is 1969-12-29 that is beyond the limits)
    DateTime::MakeTimestamp(DateTime::StartOfWeek(Date("2019-01-01"))),
      -- 2018-12-31T00:00:00Z
    DateTime::MakeDatetime(DateTime::StartOfDay(Datetime("2019-06-06T01:02:03Z"))),
      -- 2019-06-06T00:00:00Z
    DateTime::MakeTzDatetime(DateTime::StartOfDay(TzDatetime("1970-01-01T05:00:00,Europe/Moscow"))),
      -- NULL (beyond the epoch in GMT)
    DateTime::MakeTzTimestamp(DateTime::StartOfDay(TzTimestamp("1970-01-02T05:00:00.000000,Europe/Moscow"))),
      -- 1970-01-02T00:00:00,Europe/Moscow (the beginning of the day in Moscow)
    DateTime::MakeDatetime(DateTime::StartOf(Datetime("2019-06-06T23:45:00Z"), Interval("PT7H"))),
      -- 2019-06-06T21:00:00Z
    DateTime::MakeDatetime(DateTime::StartOf(Datetime("2019-06-06T23:45:00Z"), Interval("PT20M"))),
      -- 2019-06-06T23:40:00Z
    DateTime::TimeOfDay(Timestamp("2019-02-14T01:02:03.456789Z"));
      -- 1 hour 2 minutes 3 seconds 456789 microseconds
```

## Shift... {#shift}

Add/subtract the specified number of units to/from the component in the internal representation and update the other fields.
Returns either an updated copy or NULL, if an update produces an invalid date or other inconsistencies.

**List of functions**

* ```DateTime::ShiftYears(Resource<TM>{Flags:AutoMap}, Int32) -> Resource<TM>?```
* ```DateTime::ShiftQuarters(Resource<TM>{Flags:AutoMap}, Int32) -> Resource<TM>?```
* ```DateTime::ShiftMonths(Resource<TM>{Flags:AutoMap}, Int32) -> Resource<TM>?```

If the resulting number of the day in the month exceeds the maximum allowed, then the `Day` field will accept the last day of the month without changing the time (see examples).

**Examples**

```yql
$tm1 = DateTime::Split(DateTime("2019-01-31T01:01:01Z"));
$tm2 = DateTime::Split(TzDatetime("2049-05-20T12:34:50,Europe/Moscow"));

SELECT
    DateTime::MakeDate(DateTime::ShiftYears($tm1, 10)), -- 2029-01-31T01:01:01
    DateTime::MakeDate(DateTime::ShiftYears($tm2, -10000)), -- NULL (beyond the limits)
    DateTime::MakeDate(DateTime::ShiftQuarters($tm2, 0)), -- 2049-05-20T12:34:50,Europe/Moscow
    DateTime::MakeDate(DateTime::ShiftQuarters($tm1, -3)), -- 2018-04-30T01:01:01
    DateTime::MakeDate(DateTime::ShiftMonths($tm1, 1)), -- 2019-02-28T01:01:01
    DateTime::MakeDate(DateTime::ShiftMonths($tm1, -35)), -- 2016-02-29T01:01:01
```

## Format {#format}

Get a string representation of a time using an arbitrary formatting string.

**List of functions**

* ```DateTime::Format(String) -> (Resource<TM>{Flags:AutoMap}) -> String```

A subset of specifiers similar to strptime is implemented for the formatting string.

* `%%`: % character.
* `%Y`: 4-digit year.
* `%m`: 2-digit month.
* `%d`: 2-digit day.
* `%H`: 2-digit hour.
* `%M`: 2-digit minutes.
* `%S`: 2-digit seconds  -- or xx.xxxxxx  in the case of non-empty microseconds.
* `%z`: +hhmm or -hhmm.
* `%Z`: IANA name of the timezone.
* `%b`: A short three-letter English name of the month (Jan).
* `%B`: A full English name of the month (January).

All other characters in the format string are passed on without changes.

**Examples**

```yql
$format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

SELECT
    $format(DateTime::Split(TzDatetime("2019-01-01T01:02:03,Europe/Moscow")));
      -- "2019-01-01 01:02:03 Europe/Moscow"
```

## Parse {#parse}

Parse a string into an internal representation using an arbitrary formatting string. Default values are used for empty fields. If errors are raised, NULL is returned.

**List of functions**

* ```DateTime::Parse(String) -> (String{Flags:AutoMap}) -> Resource<TM>?```

Implemented specifiers:

* `%%`: the % character.
* `%Y`: 4-digit year (1970).
* `%m`: 2-digit month (1).
* `%d`: 2-digit day (1).
* `%H`: 2-digit hour (0).
* `%M`: 2-digit minutes (0).
* `%S`: Seconds (0), can also accept microseconds in the formats from xx. up to xx.xxxxxx
* `%Z`: The IANA name of the timezone (GMT).
* `%b`: A short three-letter case-insensitive English name of the month (Jan).
* `%B`: A full case-insensitive English name of the month (January).

**Examples**

```yql
$parse1 = DateTime::Parse("%H:%M:%S");
$parse2 = DateTime::Parse("%S");
$parse3 = DateTime::Parse("%m/%d/%Y");
$parse4 = DateTime::Parse("%Z");

SELECT
    DateTime::MakeDatetime($parse1("01:02:03")), -- 1970-01-01T01:02:03Z
    DateTime::MakeTimestamp($parse2("12.3456")), -- 1970-01-01T00:00:12.345600Z
    DateTime::MakeTimestamp($parse3("02/30/2000")), -- NULL (Feb 30)
    DateTime::MakeTimestamp($parse4("Canada/Central")); -- 1970-01-01T06:00:00Z (conversion to UTC)
```

For the common formats, wrappers around the corresponding util methods are supported. You can only get TM with components in the UTC timezone.

**List of functions**

* ```DateTime::ParseRfc822(String{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::ParseIso8601(String{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::ParseHttp(String{Flags:AutoMap}) -> Resource<TM>?```
* ```DateTime::ParseX509(String{Flags:AutoMap}) -> Resource<TM>?```

**Examples**

```yql
SELECT
    DateTime::MakeTimestamp(DateTime::ParseRfc822("Fri, 4 Mar 2005 19:34:45 EST")),
      -- 2005-03-05T00:34:45Z
    DateTime::MakeTimestamp(DateTime::ParseIso8601("2009-02-14T02:31:30+0300")),
      -- 2009-02-13T23:31:30Z
    DateTime::MakeTimestamp(DateTime::ParseHttp("Sunday, 06-Nov-94 08:49:37 GMT")),
      -- 1994-11-06T08:49:37Z
    DateTime::MakeTimestamp(DateTime::ParseX509("20091014165533Z"))
      -- 2009-10-14T16:55:33Z
```

## Standard scenarios

**Conversions between strings and seconds**

Converting a string date (in the Moscow timezone) to seconds (in GMT timezone):

```yql
$datetime_parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");
$datetime_parse_tz = DateTime::Parse("%Y-%m-%d %H:%M:%S %Z");

SELECT
    DateTime::ToSeconds(TzDateTime("2019-09-16T00:00:00,Europe/Moscow")) AS md_us1, -- 1568581200
    DateTime::ToSeconds(DateTime::MakeDatetime($datetime_parse_tz("2019-09-16 00:00:00" || " Europe/Moscow"))),  -- 1568581200
    DateTime::ToSeconds(DateTime::MakeDatetime(DateTime::Update($datetime_parse("2019-09-16 00:00:00"), "Europe/Moscow" as Timezone))), -- 1568581200

    -- INCORRECT (Date imports time as GMT, but AddTimezone has no effect on ToSeconds that always returns GMT time)
    DateTime::ToSeconds(AddTimezone(Date("2019-09-16"), 'Europe/Moscow')) AS md_us2, -- 1568592000
```

Converting a string date (in the Moscow timezone) to seconds (in the Moscow timezone). DateTime::ToSeconds() exports only to GMT. That's why we should put timezones aside for a while and use only GMT (as if we assumed for a while that Moscow is in GMT):

```yql
$date_parse = DateTime::Parse("%Y-%m-%d");
$datetime_parse = DateTime::Parse("%Y-%m-%d %H:%M:%S");
$datetime_parse_tz = DateTime::Parse("%Y-%m-%d %H:%M:%S %Z");

SELECT
    DateTime::ToSeconds(Datetime("2019-09-16T00:00:00Z")) AS md_ms1, -- 1568592000
    DateTime::ToSeconds(Date("2019-09-16")) AS md_ms2, -- 1568592000
    DateTime::ToSeconds(DateTime::MakeDatetime($date_parse("2019-09-16"))) AS md_ms3, -- 1568592000
    DateTime::ToSeconds(DateTime::MakeDatetime($datetime_parse("2019-09-16 00:00:00"))) AS md_ms4, -- 1568592000
    DateTime::ToSeconds(DateTime::MakeDatetime($datetime_parse_tz("2019-09-16 00:00:00 GMT"))) AS md_ms5, -- 1568592000

    -- INCORRECT (imports the time in the Moscow timezone, but RemoveTimezone doesn't affect ToSeconds in any way)
    DateTime::ToSeconds(RemoveTimezone(TzDatetime("2019-09-16T00:00:00,Europe/Moscow"))) AS md_ms6, -- 1568581200
    DateTime::ToSeconds(DateTime::MakeDatetime($datetime_parse_tz("2019-09-16 00:00:00 Europe/Moscow"))) AS md_ms7 -- 1568581200
```

Converting seconds (in the GMT timezone) to a string date (in the Moscow timezone):

```yql
$date_format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");
SELECT
    $date_format(AddTimezone(DateTime::FromSeconds(1568592000), 'Europe/Moscow')) -- "2019-09-16 03:00:00 Europe/Moscow"
```

Converting seconds (in the Moscow timezone) to a string date (in the Moscow timezone). In this case, the %Z  timezone is output for reference: usually, it's not needed because it's "GMT" and might mislead you.

```yql
$date_format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");
SELECT
    $date_format(DateTime::FromSeconds(1568592000)) -- "2019-09-16 00:00:00 GMT"
```

Converting seconds (in the GMT timezone) to three-letter days of the week (in the Moscow timezone):

```yql
SELECT
    SUBSTRING(DateTime::GetDayOfWeekName(AddTimezone(DateTime::FromSeconds(1568581200), "Europe/Moscow")), 0, 3) -- "Mon"
```

**Date and time formatting**

Usually a separate named expression is used to format time, but you can do without it:

```yql
$date_format = DateTime::Format("%Y-%m-%d %H:%M:%S %Z");

SELECT

   -- A variant with a named expression

   $date_format(AddTimezone(DateTime::FromSeconds(1568592000), 'Europe/Moscow')),

   -- A variant without a named expression

   DateTime::Format("%Y-%m-%d %H:%M:%S %Z")
       (AddTimezone(DateTime::FromSeconds(1568592000), 'Europe/Moscow'))
;
```

**Converting types**

This way, you can convert only constants:

```yql
SELECT
    TzDateTime("2019-09-16T00:00:00,Europe/Moscow"), -- 2019-09-16T00:00:00,Europe/Moscow
    Date("2019-09-16") -- 2019-09-16
```

But this way, you can convert a constant, a named expression, or a table field:

```yql
SELECT
    CAST("2019-09-16T00:00:00,Europe/Moscow" AS TzDateTime), -- 2019-09-16T00:00:00,Europe/Moscow
    CAST("2019-09-16" AS Date) -- 2019-09-16
```

**Converting time to date**

A CAST to Date or TzDate outputs a GMT date for a midnight, local time (for example, for Moscow time 2019-10-22 00:00:00, the date 2019-10-21 is returned). To get a date in the local timezone, you can use DateTime::Format.

```yql
$x = DateTime("2019-10-21T21:00:00Z");
select
    AddTimezone($x, "Europe/Moscow"), -- 2019-10-22T00:00:00,Europe/Moscow
    cast($x as TzDate), -- 2019-10-21,GMT
    cast(AddTimezone($x, "Europe/Moscow") as TzDate), -- 2019-10-21,Europe/Moscow
    cast(AddTimezone($x, "Europe/Moscow") as Date), -- 2019-10-21
  DateTime::Format("%Y-%m-%d %Z")(AddTimezone($x, "Europe/Moscow")), -- 2019-10-22 Europe/Moscow
```

It's worth mentioning that several `TzDatetime` or `TzTimestamp` values with a positive timezone offset cannot be cast to `TzDate`. Consider the example below:

```yql
SELECT CAST(TzDatetime("1970-01-01T23:59:59,Europe/Moscow") as TzDate);
/* Fatal: Timestamp 1970-01-01T23:59:59.000000,Europe/Moscow cannot be casted to TzDate */
```

Starting from the Unix epoch, there is no valid value representing midnight on 01/01/1970 for the Europe/Moscow timezone. As a result, such a cast is impossible and fails at runtime.

At the same time, values with a negative timezone offset are converted correctly:

```yql
SELECT CAST(TzDatetime("1970-01-01T23:59:59,America/Los_Angeles") as TzDate);
/* 1970-01-01,America/Los_Angeles */

```

**Daylight saving time**

Please note that daylight saving time depends on the year:

```yql
SELECT
    RemoveTimezone(TzDatetime("2019-09-16T10:00:00,Europe/Moscow")) as DST1, -- 2019-09-16T07:00:00Z
    RemoveTimezone(TzDatetime("2008-12-03T10:00:00,Europe/Moscow")) as DST2, -- 2008-12-03T07:00:00Z
    RemoveTimezone(TzDatetime("2008-07-03T10:00:00,Europe/Moscow")) as DST3, -- 2008-07-03T06:00:00Z (DST)
```

