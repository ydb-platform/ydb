# SELECT syntax

<!-- File split by includable blocks as part of YQL docs preparation for YQL/YDB opensource -->

{% include [x](_includes/select/calc.md) %}

{% include [x](_includes/select/from.md) %}

{% if feature_secondary_index %}

  {% include [x](_includes/select/secondary_index.md) %}

{% endif %}

{% include [x](_includes/select/with.md) %}

{% include [x](_includes/select/where.md) %}

{% include [x](_includes/select/order_by.md) %}

{% include [x](_includes/select/limit_offset.md) %}

{% include [x](_includes/select/assume_order_by.md) %}

{% include [x](_includes/select/sample.md) %}

{% include [x](_includes/select/distinct.md) %}

{% include [x](_includes/select/execution.md) %}

{% include [x](_includes/select/column_order.md) %}

{% include [x](_includes/select/combining_queries.md) %}

{% include [x](_includes/select/union_all.md) %}

{% include [x](_includes/select/union.md) %}

{% include [x](_includes/select/commit.md) %}

{% if feature_bulk_tables %}

  {% include [x](_includes/select/functional_tables.md) %}

  {% include [x](_includes/select/folder.md) %}

{% endif %}

{% include [x](_includes/select/without.md) %}

{% include [x](_includes/select/from_select.md) %}

{% if feature_map_reduce %}

  {% include [x](_includes/select/view.md) %}

{% endif %}

{% if feature_temp_table %}

  {% include [x](_includes/select/temporary_table.md) %}

{% endif %}

{% include [x](_includes/select/from_as_table.md) %}

