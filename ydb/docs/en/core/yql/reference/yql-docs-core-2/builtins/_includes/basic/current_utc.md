## CurrentUtc... {#current-utc}

`CurrentUtcDate()`, `CurrentUtcDatetime()` and `CurrentUtcTimestamp()`: Getting the current date and/or time in UTC. The result data type is specified at the end of the function name.

The arguments are optional and work same as [RANDOM](#random).

**Examples**

```yql
SELECT CurrentUtcDate();
```

```yql
SELECT CurrentUtcTimestamp(TableRow()) FROM my_table;
```

