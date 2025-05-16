{% note alert %}

The functionality of vector indexes is available in the test mode in main. This functionality will be fully available in version 25.1.

The following features are not supported:

* modifying rows in tables with vector indexes
* building an index for vectors with [bit quantization](../yql/reference/udf/list/knn.md#functions-convert)

These limitations may be removed in future versions.

{% endnote %}
