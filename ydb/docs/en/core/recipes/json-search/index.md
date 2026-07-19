# JSON document search recipes

This section contains ready-to-use recipes for [JSON indexes](../../dev/json-indexes.md) to efficiently select data by the content of `Json` and `JsonDocument` columns.

The recipes are built around the [JSON_EXISTS](../../yql/reference/builtins/json.md) and [JSON_VALUE](../../yql/reference/builtins/json.md) functions with [JsonPath](../../yql/reference/builtins/json.md#jsonpath) expressions. To guarantee index use, all examples explicitly address the index via `VIEW IndexName`.

* [{#T}](json-index-quickstart.md) — minimal scenario: create a table and index, insert data, and run basic queries.
* [{#T}](json-index-catalog.md) — product catalog example with nested attributes, range conditions, and search over nested arrays.
* [{#T}](json-index-parameters.md) — parameterized queries and passing variables into JsonPath via the `PASSING` section.
* [{#T}](json-index-typecheck.md) — checking field types and path existence with JsonPath methods.

Before running the examples, make sure [JSON index support is enabled](../../reference/configuration/feature_flags.md) on the cluster.
