|Setting name|Description|Possible values|
|----|----|---|
|`file_pattern`|File name template|File name template string. Wildcards `*` are supported.|
|`data.interval.unit`|Unit for parsing `Interval` type|`MICROSECONDS`, `MILLISECONDS`, `SECONDS`, `MINUTES`, `HOURS`, `DAYS`, `WEEKS`|
|`data.datetime.format_name`|Predefined format in which `Datetime` data is stored|`POSIX`, `ISO`|
|`data.datetime.format`|Strftime-like template which defines how `Datetime` data is stored|Formatting string, for example: `%Y-%m-%dT%H-%M`|
|`date.timestamp.format_name`|Predefined format in which `Timestamp` data is stored|`POSIX`, `ISO`, `UNIX_TIME_MILLISECONDS`, `UNIX_TIME_SECONDS`, `UNIX_TIME_MICROSECONDS`|
|`data.timestamp.format`|Strftime-like template which defines how `Timestamp` data is stored|Formatting string, for example: `%Y-%m-%dT%H-%M-%S`|
|`data.date.format`|The format in which `Date` data is stored|Formatting string, for example: `%Y-%m-%d`|
|`csv_delimiter`|Delimeter for `csv_with_names` format|Any character|
