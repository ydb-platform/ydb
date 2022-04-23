# Aggregate functions

{% include [simple.md](_includes/aggregation/simple.md) %}

{% include [count_distinct_estimate.md](_includes/aggregation/count_distinct_estimate.md) %}

{% include [agg_list.md](_includes/aggregation/agg_list.md) %}

{% include [max_min_by.md](_includes/aggregation/max_min_by.md) %}

{% include [top_bottom.md](_includes/aggregation/top_bottom.md) %}

{% include [topfreq_mode.md](_includes/aggregation/topfreq_mode.md) %}

{% include [stddev_variance.md](_includes/aggregation/stddev_variance.md) %}

{% include [corr_covar.md](_includes/aggregation/corr_covar.md) %}

{% include [percentile_median.md](_includes/aggregation/percentile_median.md) %}

{% include [histogram.md](_includes/aggregation/histogram.md) %}

{% include [bool_bit.md](_includes/aggregation/bool_bit.md) %}

{% if feature_window_functions %}

  {% include [session_start.md](_includes/aggregation/session_start.md) %}

{% endif %}

{% include [aggregate_by.md](_includes/aggregation/aggregate_by.md) %}
