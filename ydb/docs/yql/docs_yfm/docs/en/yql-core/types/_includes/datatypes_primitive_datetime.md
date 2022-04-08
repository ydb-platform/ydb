| Type | Description | Notes |
| ----- | ----- | ----- |
| `Date` | Date, precision to the day | Range of values for all time types except `Interval`: From 00:00 01.01.1970 to 00:00 01.01.2106. Internal `Date` representation: Unsigned 16-bit integer |
| `Datetime` | Date/time, precision to the second | Internal representation: Unsigned 32-bit integer |
| `Timestamp` | Date/time, precision to the microsecond | Internal representation: Unsigned 64-bit integer |
| `Interval` | Time interval (signed), precision to microseconds | Value range: From -136 years to +136 years. Internal representation: Signed 64-bit integer. {% if feature_map_tables %}Can't be used in the primary key{% endif %} |
| `TzDate` | Date with time zone label, precision to the day | Not supported in table columns |
| `TzDateTime` | Date/time with time zone label, precision to the second | Not supported in table columns |
| `TzTimestamp` | Date/time with time zone label, precision to the microsecond | Not supported in table columns |

