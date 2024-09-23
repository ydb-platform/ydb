## TOPFREQ and MODE {#topfreq-mode}

Getting an **approximate** list of the most common values in a column with an estimation of their count. Returns a list of structures with two fields:

* `Value`: the frequently occurring value that was found.
* `Frequency`: An estimated value occurrence in the table.

Required argument: the value itself.

Optional arguments:

1. For `TOPFREQ`, the desired number of items in the result. `MODE` is an alias to `TOPFREQ` with this argument set to 1. For `TOPFREQ`, this argument is also 1 by default.
2. The number of items in the buffer used: lets you trade memory consumption for accuracy. Default: 100.

**Examples**

```yql
SELECT
    MODE(my_column),
    TOPFREQ(my_column, 5, 1000)
FROM my_table;
```

