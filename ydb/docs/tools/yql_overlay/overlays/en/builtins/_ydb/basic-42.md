1. A string in double quotes with the name of an aggregate function, for example ["MIN"](../aggregation.md#min-max).
2. Optional parameters of the aggregate function that are data-independent. For example, the percentile value in [PERCENTILE](../aggregation.md#percentile-median).

The resulting factory can be used as the second parameter of the function [AGGREGATE_BY](../aggregation.md#aggregate-by).
If the aggregate function is applied to two columns instead of one, as, for example, [MIN_BY](../aggregation.md#max-min-by), then in [AGGREGATE_BY](../aggregation.md#aggregate-by), the first argument passes a `Tuple` of two values. See more details in the description of the applicable aggregate function.

### Examples
