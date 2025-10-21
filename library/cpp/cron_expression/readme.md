
CronExpression
=============

Implementation
--------------
This reference is based on:
- [exander77's supertinycron on GitHub](https://github.com/exander77/supertinycron/blob/master/README.md#supertinycron)

Overview
--------------

```
Field name     Mandatory?   Allowed values          Allowed special characters
----------     ----------   --------------          -------------------------
Second         No           0-59                    * / , - L
Minute         Yes          0-59                    * / , -
Hour           Yes          0-23                    * / , -
Day of month   Yes          1-31                    * / , - L W
Month          Yes          1-12 or JAN-DEC         * / , -
Day of week    Yes          0-6 or SUN-SAT          * / , - L #
Year           No           1970–2199               * / , -
```

### Special Characters

#### Asterisk `*`
The asterisk indicates that the cron expression matches all values of the field. For instance, an asterisk in the 'Month' field matches every month.

#### Hyphen `-`
Hyphens define ranges. For instance, `2000-2010` in the 'Year' field matches every year from 2000 to 2010, inclusive.

#### Slash `/`
Slashes specify increments within ranges. For example, `3-59/15` in the 'Minute' field matches the third minute of the hour and every 15 minutes thereafter. The form `*/...` is equivalent to `first-last/...`, representing an increment over the full range of the field. (Example: `*/40` in minute field means repeating every time when minutes amount matches `0` or `40`, but not every `40` minutes!)

#### Comma `,`
Commas separate items in a list. For instance, `MON,WED,FRI` in the 'Day of week' field matches Mondays, Wednesdays, and Fridays.

#### `L`
The character `L` stands for "last". In the 'Day of week' field, `5L` denotes the last Friday of a given month. In the 'Day of month' field, it represents the last day of the month.

- Using `L` alone in the 'Day of week' field is equivalent to `0` or `SAT`. Hence, expressions `* * * * * L *` and `* * * * * 0 *` are the same.
  
- When followed by another value in the 'Day of week' field, like `5L`, it signifies the last Friday of the month.
  
- If followed by a negative number in the 'Day of month' field, such as `L-3`, it indicates the third-to-last day of the month.

Using 'L' with other specifying lists or ranges is allowed, in this case different requirements must be satisfied at the sane time (Example: `* * * L 1 L` indicates dates, when `31JAN` is `SAT`).

#### `W`
The `W` character is exclusive to the 'Day of month' field. It indicates the closest business day (Monday-Friday) to the given day. For example, `15W` means the nearest business day to the 15th of the month. If you set 1W for the day-of-month and the 1st falls on a Saturday, the trigger activates on Monday the 3rd, since it respects the month's day boundaries and won't skip over them. Similarly, at the end of the month, the behavior ensures it doesn't "jump" over the boundary to the following month.

The `W` character can also pair with `L` (as `LW`), signifying the last business day of the month. Alone, it's equivalent to the range `1-5`, making the expressions `* * * W * * *` and `* * * * * 1-5 *` identical.

#### Hash `#`
The `#` character is only for the 'Day of week' field and should be followed by a number between one and five, or their negative values. It lets you specify constructs like "the second Friday" of a month.

For example, `6#3` means the third Friday of the month. Note that if you use `#5` and there isn't a fifth occurrence of that weekday in the month, no firing occurs for that month. Using the '#' character requires a single expression in the 'Day of week' field.

Negative nth values are also valid. For instance, `6#-1` is equivalent to `6L`.

Predefined cron expressions
---------------------------
(Copied from <https://en.wikipedia.org/wiki/Cron#Predefined_scheduling_definitions>, with text modified according to this implementation)

    Entry       Description                                                             Equivalent to
    @annually   Run once a year at midnight in the morning of January 1                 0 0 0 1 1 *
    @yearly     Run once a year at midnight in the morning of January 1                 0 0 0 1 1 *
    @monthly    Run once a month at midnight in the morning of the first of the month   0 0 0 1 * *
    @weekly     Run once a week at midnight in the morning of Sunday                    0 0 0 * * 0
    @daily      Run once a day at midnight                                              0 0 0 * * *
    @hourly     Run once an hour at the beginning of the hour                           0 0 * * * *
    @minutely   Run once a minute at the beginning of minute                            0 * * * * *
    @secondly   Run once every second                                                   * * * * * * *
    @reboot     Not supported

Other details
-------------
* If only five fields are present, the Year and Second fields are omitted. The omitted Year and Second are `*` and `0` respectively.
* If only six fields are present, the Year field is omitted. The omitted Year is set to `*`.

Usage
-----

TCronExpression has a constuctor from cron expression. Then has two methods: CronNext(TInstant), CronPrev(TInstant), which returnes next (previous) appropriate date from given.

Examples of supported expressions
---------------------------------

Expression, input date, next date:

    "*/15 * 1-4 * * *",  "2012-07-01_09:53:50", "2012-07-02_01:00:00"
    "0 */2 1-4 * * *",   "2012-07-01_09:00:00", "2012-07-02_01:00:00"
    "0 0 7 ? * MON-FRI", "2009-09-26_00:42:55", "2009-09-28_07:00:00"
    "0 */40 * * * *",  "2004-09-01_23:46:00", "2004-09-02_00:00:00"
    "0 30 23 30 1/3 ?",  "2011-04-30_23:30:00", "2011-07-30_23:30:00"

See more examples in /ut.
