### Signature


```yql
PERCENTILE(T, Double)->T
PERCENTILE(T, Tuple<Double, ...>)->Tuple<T, ...>
PERCENTILE(T, Struct<name1:Double, ...>)->Struct<name1:T, ...>
PERCENTILE(T, List<Double>)->List<T>

MEDIAN(T, [ Double ])->T
MEDIAN(T, [ Tuple<Double, ...> ])->Tuple<T, ...>
MEDIAN(T, [ Struct<name1:Double, ...> ])->Struct<name1:T, ...>
MEDIAN(T, [ List<Double> ])->List<T>
```


Calculating percentiles using the amortized version of the [TDigest](https://github.com/tdunning/t-digest) algorithm. `MEDIAN(x)` without the second argument is an alias for `PERCENTILE(x, 0.5)`.
`MEDIAN` with two arguments is fully equivalent to `PERCENTILE`.

The first argument of `PERCENTILE`/`MEDIAN` accepts an expression of type `T`. Currently supported types for `T` are `Interval` and `Double` (as well as types that allow implicit conversion to them, such as integer types).

As the second argument, you can use either a single `Double` (the percentile value) or several percentile values at once as a `Tuple`/`Struct`/`List`.

Percentile values must be in the range from 0.0 to 1.0 inclusive.

### Examples


```yql
SELECT
    MEDIAN(numeric_column),
    PERCENTILE(numeric_column, 0.99),
    PERCENTILE(CAST(string_column as Double), (0.01, 0.5, 0.99)),                   -- calculate three percentiles at once
    PERCENTILE(numeric_column, AsStruct(0.01 as p01, 0.5 as median, 0.99 as p99)), -- using a struct, percentile values can be given convenient names
    PERCENTILE(numeric_column, ListFromRange(0.00, 1.05, 0.05)),                   -- calculate multiple percentiles (from 0.0 to 1.0 inclusive with step 0.05)
FROM my_table;
```
