# JSON document search recipes

This section contains ready-made recipes for using [JSON indexes](../../dev/json-indexes.md) for efficient data selection based on the contents of columns of type `Json` and `JsonDocument`.

The recipes are built around the functions [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) with [JsonPath](../../yql/reference/builtins/json.md#jsonpath) expressions. To guarantee index usage, all examples use an explicit reference via `VIEW IndexName`.

* [{#T}](json-index-quickstart.md) — minimal scenario: creating a table, an index, inserting data, and basic queries.
* [{#T}](json-index-catalog.md) — example of a product catalog with nested attributes, range conditions, and search within nested arrays.
* [{#T}](json-index-parameters.md) — parameterized queries and passing variables to JsonPath via the `PASSING` section.
* [{#T}](json-index-typecheck.md) — checking field type and path existence using JsonPath methods.

Before running the examples, make sure that JSON index support is enabled on the cluster [JSON index support is enabled](../../reference/configuration/feature_flags.md).
