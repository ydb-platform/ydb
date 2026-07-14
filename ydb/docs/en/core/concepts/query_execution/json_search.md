# Searching JSON document content

JSON search is a way to find table rows by the content of a JSON document stored in a column of type `Json` or `JsonDocument`: by the existence of a path in the document and by the value located at the specified path. The path is specified by a [JsonPath](../../yql/reference/builtins/json.md#jsonpath) expression, and the checks are performed by the existing functions [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../../yql/reference/builtins/json.md#json_value). Typical scenarios:

* Search for documents with a specific nested field
* Search for documents where the field at a given path equals the required value
* filtering semi-structured data without a pre-defined schema.

In {{ ydb-short-name }}, JSON search can be performed in two main ways:

* without an index — by scanning the table and applying the `JSON_EXISTS` / `JSON_VALUE` functions to each row. The approach is simple but scales poorly: the amount of scanning work grows with the table size.
* with a JSON index — creating a [JSON index](../../dev/json-indexes.md) on a JSON column. This approach is designed for scalable search.

## JSON search with a JSON index {#json-search-index}

The key idea of the [JSON index](../../dev/json-indexes.md) is building an [inverted index](https://en.wikipedia.org/wiki/Inverted_index) by paths and path+value pairs. Each path of a JSON document (and for equality checks, the scalar value at that path) is encoded into a token. For each token, the index stores a list of primary key values of the corresponding table rows. Queries to the index are reduced to an inverted search over these tokens — following the same scheme as the [full-text index](../../dev/fulltext-indexes.md#basic), but with its own JSON tokenizer.

A JSON index does not store a copy of the entire JSON document, but decomposes it into individual tokens:

* Create a token 'path exists' for each path in the JSON tree.
* if the path leads to a scalar value (string, number, boolean value, or `null`), additionally, a token "path + value" is created.

Arrays are transparent: array elements are indexed under the same path as the array itself, so the index responds equally to queries to `$.items`, `$.items[0]`, and `$.items[*]`.

For example, for the document:


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


Conceptually, the following tokens are indexed (without considering the specifics of the internal representation):

| Token | What it allows to find |
| --- | --- |
| `$` | document is not equal to `NULL` |
| `$.id` / `$.id == 42042` | path `id` exists / equals `42042` |
| `$.name` / `$.name == "Michael"` | path `name` exists or equals `"Michael"` |
| `$.email` / `$.email == null` | path `email` exists / equals `null` |
| `$.items` / `$.items == null` / `$.items == "str"` | path `items` exists / contains element `null` / contains element `"str"` |
| `$.parts` | path `parts` exists |
| `$.parts.key` / `$.parts.key == "k1"` | nested path exists / equals `"k1"` |
| `$.parts.value` / `$.parts.value == false` | nested path exists / equals `false` |

JSON index allows:

* find rows by path existence using [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists)
* find rows by value in path via [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) or a filter predicate inside `JSON_EXISTS`.

When executing a query, the JSON index can be automatically used by the [optimizer](../glossary.md#optimizer). It is enough to write a regular condition `WHERE` with expressions `JSON_EXISTS` / `JSON_VALUE` on an indexed JSON column — the optimizer recognizes such a predicate and applies a JSON index read instead of scanning the source table records. Additionally, the JSON index, like any other [secondary index](../glossary.md#secondary-index), can be forced by specifying its name in the `VIEW IndexName` section.

If the predicate cannot be converted into an index reference, the behavior of {{ ydb-short-name }} depends on whether the required JSON index was explicitly specified in the query:

* When the optimizer automatically selects an index, the query is simply executed without index acceleration (the result remains correct).
* an error is returned when explicitly specifying an index via the `VIEW` expression.

For more information, see [{#T}](../../yql/reference/syntax/select/json_index.md).

Additional information:

* [JSON indexes](../../dev/json-indexes.md) — overview of supported predicates and limitations.
* [JSON functions](../../yql/reference/builtins/json.md): reference for `JSON_EXISTS`, `JSON_VALUE`, and the JsonPath language.
* [VIEW (JSON index)](../../yql/reference/syntax/select/json_index.md): syntax for queries with a JSON index.
