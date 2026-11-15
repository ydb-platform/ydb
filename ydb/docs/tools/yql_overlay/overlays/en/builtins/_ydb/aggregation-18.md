### Signature


```yql
HISTOGRAM(Double?)->HistogramStruct?
HISTOGRAM(Double?, weight:Double)->HistogramStruct?
HISTOGRAM(Double?, intervals:Uint32)->HistogramStruct?
HISTOGRAM(Double?, weight:Double, intervals:Uint32)->HistogramStruct?
```


In the signature descriptions, `HistogramStruct` refers to the result of the aggregate function, which is a structure of a specific kind.

Plotting an approximate histogram based on a numeric expression with automatic selection of buckets.

[Auxiliary functions](../../udf/list/histogram.md)
