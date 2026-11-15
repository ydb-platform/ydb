### Signature


```yql
LinearHistogram(Double?)->HistogramStruct?
LinearHistogram(Double? [, binSize:Double [, min:Double [, max:Double]]])->HistogramStruct?

LogarithmicHistogram(Double?)->HistogramStruct?
LogarithmicHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
LogHistogram(Double?)->HistogramStruct?
LogHistogram(Double? [, logBase:Double [, min:Double [, max:Double]]])->HistogramStruct?
```


Arguments:

1. Expression used to plot the histogram. All the following arguments are optional.
2. Spacing between the `LinearHistogram` buckets or the logarithm base for `LogarithmicHistogram`/`LogHistogram` (those are aliases). In both cases, the default value is 10.
3. Minimum value. By default, it's minus infinity.
4. Maximum value. By default, it's plus infinity.

The format of the result is totally similar to [adaptive histograms](#histogram), so you can use the same [set of auxiliary functions](../../udf/list/histogram.md).

If the spread of input values is uncontrollably large, we recommend that you specify the minimum and maximum values to prevent potential failures due to high memory consumption.

### Examples


```yql
SELECT
    LogarithmicHistogram(numeric_column, 2)
FROM my_table;
```


## CDF (cumulative distribution function) {#histogramcdf}

The suffix `CDF` can be appended to each type of Histogram function to build a cumulative distribution function. The constructs


```yql
SELECT
    Histogram::ToCumulativeDistributionFunction(Histogram::Normalize(<histogram_function>Histogram(numeric_column)))
FROM my_table;
```


and


```yql
SELECT
    <histogram_function>HistogramCDF(numeric_column)
FROM my_table;
```


are fully equivalent.
