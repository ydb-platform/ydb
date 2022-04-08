## HISTOGRAM {#histogram}

Plotting an approximate histogram based on a numeric expression with automatic selection of buckets.

[Auxiliary functions](../../../udf/list/histogram.md)

### Basic settings

You can limit the number of buckets using an optional argument. The default value is 100. Keep in mind that added accuracy costs you more computing resources and may negatively affect the query execution time. In extreme cases, it may affect your query success.

### Support for weights

You can specify a "weight" for each value used in the histogram. To do this, pass to the aggregate function the second argument with an expression for calculating the weight. The weight of `1.0` is always used by default. If you use non-standard weights, you may also use the third argument to limit the number of buckets.

If you pass two arguments, the meaning of the second argument is determined by its type (if it's an integer literal, it limits the number of buckets, otherwise it's used as a weight).

{% if tech %}

### Algorithms

* [Source whitepaper](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf);

Various modifications of the algorithm are available:

```yql
AdaptiveDistanceHistogram
AdaptiveWeightHistogram
AdaptiveWardHistogram
BlockWeightHistogram
BlockWardHistogram
```

By default, `HISTOGRAM` is a synonym for `AdaptiveWardHistogram`. Both functions are equivalent and interchangeable in all contexts.

The Distance, Weight, and Ward algorithms differ in the formulas that combine two points into one:

```c++
    TWeightedValue CalcDistanceQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.first - left.first, left.first);
    }

    TWeightedValue CalcWeightQuality(const TWeightedValue& left, const TWeightedValue& right) {
        return TWeightedValue(right.second + left.second, left.first);
    }

    TWeightedValue CalcWardQuality(const TWeightedValue& left, const TWeightedValue& right) {
        const double N1 = left.second;
        const double N2 = right.second;
        const double mu1 = left.first;
        const double mu2 = right.first;
        return TWeightedValue(N1 * N2 / (N1 + N2) * (mu1 - mu2) * (mu1 - mu2), left.first);
    }
```

Difference between Adaptive and Block:

<blockquote>Contrary to adaptive histogram, block histogram doesn't rebuild bins after each point is added. Instead, it accumulates points and if the amount of points overflows specified limits, it shrinks all the points at once to produce a histogram. Indeed, there exist two limits and two shrinkage operations:

1. FastGreedyShrink is fast but coarse. It is used to shrink from upper limit to intermediate limit (override FastGreedyShrink to set specific behaviour).
2. SlowShrink is slow, but produces finer histogram. It shrinks from the intermediate limit to the actual number of bins in a manner similar to that in adaptive histogram (set CalcQuality in 34constuctor)
While FastGreedyShrink is used most of the time, SlowShrink is mostly used for histogram finalization

</blockquote>
{% endif %}

### If you need an accurate histogram

1. You can use the aggregate functions described below with fixed bucket grids: [LinearHistogram](#linearhistogram) or [LogarithmicHistogram](#logarithmichistogram).
2. You can calculate the bucket number for each row and apply to it [GROUP BY](../../../syntax/group_by.md).

When you use [aggregation factories](../../basic.md#aggregationfactory), a `Tuple` containing a value and a weight is passed as the first [AGGREGATE_BY](#aggregateby) argument.

**Examples**

```yql
SELECT
    HISTOGRAM(numeric_column)
FROM my_table;
```

```yql
SELECT
    Histogram::Print(
        HISTOGRAM(numeric_column, 10),
        50
    )
FROM my_table;
```

```yql
$hist_factory = AggregationFactory("HISTOGRAM");

SELECT
    AGGREGATE_BY(AsTuple(numeric_column, 1.0), $hist_factory)
FROM my_table;
```

## LinearHistogram, LogarithmicHistogram, and LogHistogram {#linearhistogram}

Plotting a histogram based on an explicitly specified fixed bucket scale.

Arguments:

1. Expression used to plot the histogram. All the following arguments are optional.
2. Spacing between the `LinearHistogram` buckets or the logarithm base for `LogarithmicHistogram`/`LogHistogram` (those are aliases). In both cases, the default value is 10.
3. Minimum value. By default, it's minus infinity.
4. Maximum value. By default, it's plus infinity.

The format of the result is totally similar to [adaptive histograms](#histogram), so you can use the same [set of auxiliary functions](../../../udf/list/histogram.md).

If the spread of input values is uncontrollably large, we recommend that you specify the minimum and maximum values to prevent potential failures due to high memory consumption.

**Examples**

```yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```

