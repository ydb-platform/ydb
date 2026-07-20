# Search by JSON document content

JSON search is a way to find table rows by the contents of a JSON document stored in a column of type `Json` or `JsonDocument`: by the existence of a path in the document and by the value located at the specified path. The path is defined by a [JsonPath](../../yql/reference/builtins/json.md#jsonpath) expression, and the checks are performed using the existing functions [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../../yql/reference/builtins/json.md#json_value). Typical scenarios:

* Search for documents with a specific nested field.
* Search for documents whose field at the specified path equals the required value.
* Filtering semi-structured data without a predefined schema.

In {{ ydb-short-name }} JSON search can be performed in two main ways:

* without an index — scanning the table and applying functions `JSON_EXISTS` or `JSON_VALUE` to each row. The approach is simple but scales poorly: the amount of work for scanning grows with the size of the table.
* with a JSON index — by creating a [JSON index](../../dev/json-indexes.md) on a JSON column. This approach is intended for scalable search.

## Search by JSON with JSON index {#json-search-index}

The key idea of the [JSON index](../../dev/json-indexes.md) is building an [inverted index](https://en.wikipedia.org/wiki/Inverted_index) over paths and path + value pairs. Each JSON document path (and for equality checks – the scalar value at that path) is encoded into a token. For each token, the index stores a list of primary-key values of the corresponding table rows. Queries to the index are reduced to inverted search over these tokens – using the same scheme as the [full-text index](../../dev/fulltext-indexes.md#basic), but with a dedicated JSON tokenizer.

A JSON index does not store a full copy of the JSON document, but breaks it down into individual tokens:

* For each path in the JSON tree, a “path exists” token is created.
* if the path leads to a scalar value (string, number, boolean value or `null`), an additional token "path + value" is created.

Arrays are transparent in this regard: the array elements are indexed under the same path as the array itself, so the index responds equally to queries to `$.items`, `$.items[0]`, and `$.items[*]`.

For example, for a document:


```json
{
    "id": 42042,
    "name": "Michael",
    "email": null,
    "items": [null, "str"],
    "parts": {
        "key": "k1",
        "value": false
    }
}
```


Conceptually, the following tokens are indexed (ignoring the specifics of the internal representation):

| Token | What it allows you to find |
| --- | --- |
| `$` | document is not equal to `NULL` |
| `$.id` or `$.id == 42042` | path `id` exists or equals `42042` |
| `$.name` or `$.name == "Michael"` | path `name` exists or equals `"Michael"` |
| `$.email` or `$.email == null` | The path `email` exists or equals `null` |
| `$.items` or `$.items == null` or `$.items == "str"` | Path `items` exists, or contains element `null`, or contains element `"str"` |
| `$.parts` | path `parts` exists |
| `$.parts.key` or `$.parts.key == "k1"` | nested path exists or equals `"k1"` |
| `$.parts.value` or `$.parts.value == false` | Nested path exists or equals `false` |

JSON index allows:

* Find rows based on the existence of a path using [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists)
* find rows by value in the path using [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) or a filter predicate inside `JSON_EXISTS`.

When executing a query, the JSON index can be automatically used by the [optimizer](../glossary.md#optimizer). It is enough to write a regular condition `WHERE` with expressions `JSON_EXISTS` and `JSON_VALUE` on the indexed JSON column — the optimizer recognizes such a predicate and uses JSON-index reading instead of scanning the original table's rows. Additionally, a JSON index, like any other [secondary index](../glossary.md#secondary-index), can be forced to be used by specifying its name in the `VIEW IndexName` section.

If the predicate cannot be turned into an index lookup, the behavior of {{ ydb-short-name }} depends on whether the required JSON index was explicitly specified in the query.

* When the optimizer automatically selects an index, the query is simply executed without index acceleration (the result remains correct).
* When the index is explicitly specified via the expression `VIEW`, an error is returned.

See more details [{#T}](../../yql/reference/syntax/select/json_index.md).

Additional information:

* [JSON indexes](../../dev/json-indexes.md) — overview of capabilities, supported predicates, and limitations.
* [Functions for working with JSON](../../yql/reference/builtins/json.md) — reference for `JSON_EXISTS`, `JSON_VALUE` and the JsonPath language.
* [VIEW (JSON index)](../../yql/reference/syntax/select/json_index.md) — query syntax with a JSON index.
