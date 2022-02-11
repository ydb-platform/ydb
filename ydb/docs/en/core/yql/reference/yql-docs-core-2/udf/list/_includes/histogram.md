# Histogram

Set of auxiliary functions for the [HISTOGRAM aggregate function](../../../builtins/aggregation.md). In the signature description below, HistogramStruct refers to the result of the aggregate function `HISTOGRAM`, `LinearHistogram` or `LogarithmicHistogram` being a structure of a certain type.

**List of functions**

* ```Histogram::Print(HistogramStruct{Flags:AutoMap}, Byte?) -> String```
* ```Histogram::Normalize(HistogramStruct{Flags:AutoMap}, [Double?]) -> HistogramStruct```: The second argument specifies the desired area of the histogram, 100 by default.
* ```Histogram::ToCumulativeDistributionFunction(HistogramStruct{Flags:AutoMap}) -> HistogramStruct```
* ```Histogram::GetSumAboveBound(HistogramStruct{Flags:AutoMap}, Double) -> Double```
* ```Histogram::GetSumBelowBound(HistogramStruct{Flags:AutoMap}, Double) -> Double```
* ```Histogram::GetSumInRange(HistogramStruct{Flags:AutoMap}, Double, Double) -> Double```
* ```Histogram::CalcUpperBound(HistogramStruct{Flags:AutoMap}, Double) -> Double```
* ```Histogram::CalcLowerBound(HistogramStruct{Flags:AutoMap}, Double) -> Double```
* ```Histogram::CalcUpperBoundSafe(HistogramStruct{Flags:AutoMap}, Double) -> Double```
* ```Histogram::CalcLowerBoundSafe(HistogramStruct{Flags:AutoMap}, Double) -> Double```

`Histogram::Print` has an optional numeric argument that sets the maximum length of the histogram columns (the length is in characters, since the histogram is rendered in ASCII art). Default: 25. This function is primarily intended for viewing histograms in the console{% if feature_webui %}. [The web interface](../../../interfaces/web.md) automatically makes an interactive visualization from them{% endif %}.

