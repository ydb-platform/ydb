{% note alert %}

The functionality of vector indexes is available in the test mode in main. This functionality will be fully available in version 25.1.

The following features are not supported:

* Index update: the main table can be modified, but the existing index will not be updated. A new index is to be built to reflect the changes. If necessary, the existing index can be [atomically replaced](../reference/ydb-cli/commands/secondary_index.md?version=main#rename) with the newly built one.
* Building an index for vectors with [bit quantization](../yql/reference/udf/list/knn.md#functions-convert).

These limitations may be removed in future versions.

{% endnote %}
