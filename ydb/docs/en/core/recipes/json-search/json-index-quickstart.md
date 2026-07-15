# JSON index — quick start

This guide shows how to create a [JSON index](../../dev/json-indexes.md) and execute queries using the [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) functions in {{ ydb-short-name }}.

## Create a table and JSON index


```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);
```


The column type `JsonDocument` stores JSON in a compact binary format and is preferred for an indexed column. Alternatively, the type `Json` (text representation) can be used.

The table primary key must consist of a single integer type column (`Uint64`, `Uint32`, `Int64`, or `Int32`) — this is a [current limitation](../../dev/json-indexes.md#limitations) of the JSON index implementation.

## Add test data


```yql
UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"user": {"id": 100, "name": "Alice"}, "active": true}@@)),
    (2, JsonDocument(@@{"user": {"id": 101, "name": "Bob"}, "active": false}@@)),
    (3, JsonDocument(@@{"user": {"id": 102, "name": "Charlie"}, "archived": true}@@));
```


Here, the `@@...@@` construct is a [multiline string literal](../../yql/reference/syntax/expressions.md) convenient for writing JSON without escaping quotes. The `JsonDocument(...)` function converts text into a value of type `JsonDocument`.

## Filter by presence of a path in the document

The [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) function checks whether a path specified by a JsonPath expression exists in the document.


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id');
```


Result:


```text
id
1
2
3
```


The `$.user.id` path token is used for index search. The index returns the result without scanning the main table.

## Selecting rows with a specific document field value

The [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) function extracts a scalar value using JsonPath; to use the index you must specify the return type in the `RETURNING` section:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Alice"u;
```


Result:


```text
id
1
```


When checking equality, the token “path + value” (`$.user.name = "Alice"`) is added to the index, which provides the highest selectivity.

## Combination of conditions

You can combine several `JSON_EXISTS` or `JSON_VALUE` calls on a single indexed JSON column using the `AND` and `OR` operators:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id')
  AND JSON_VALUE(payload, '$.active' RETURNING Bool);
```


Result:


```text
id
1
```


## More details

* [JSON indexes](../../dev/json-indexes.md) — a complete overview of capabilities and limitations.
* [VIEW (JSON index)](../../yql/reference/syntax/select/json_index.md) — query syntax via `VIEW`.
* [INDEX (CREATE TABLE)](../../yql/reference/syntax/create_table/json_index.md) — JSON index creation syntax.
* [{#T}](json-index-catalog.md) — example of a product catalog with nested attributes.
