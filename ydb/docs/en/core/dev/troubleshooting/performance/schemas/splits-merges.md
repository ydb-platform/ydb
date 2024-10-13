# Excessive tablet splits and merges

If the difference between min and max values exceeds 20%, Hive may start splitting tables under minimal load and then merging them back when the load is reduced.

## Diagnostics

<!-- The include is added to allow partial overrides in overlays  -->
{% include notitle [#](_includes/splits-merges.md) %}

## Recommendations

If the user load on {{ ydb-short-name }} has not changed, consider adjusting the gap between the min and max limits for the number of table partitions.
