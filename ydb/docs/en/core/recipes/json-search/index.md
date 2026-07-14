# Recipes for searching JSON documents

This section contains ready-made recipes for using [JSON indexes](../../dev/json-indexes.md) to efficiently filter data based on the contents of columns of type `Json` and `JsonDocument`.

The recipes are built around the [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) functions with [JsonPath](../../yql/reference/builtins/json.md#jsonpath) expressions. To guarantee index usage, all examples use explicit access via `VIEW IndexName`.

* [{#T}](json-index-quickstart.md) — minimal scenario: creating a table, an index, inserting data, and basic queries.
* [{#T}](json-index-catalog.md) — example of a product catalog with nested attributes, range conditions, and searching through nested arrays.
* [{#T}](json-index-parameters.md) — parameterized queries and passing variables to JsonPath via the `PASSING` section.
* [{#T}](json-index-typecheck.md) — checking the field type and path existence using JsonPath methods.

Before running the examples, make sure that [JSON index support is enabled](../../reference/configuration/feature_flags.md) on the cluster.
