# JSON index — quick start

This guide shows how to create a [JSON index](../../dev/json-indexes.md) and execute queries using the [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) and [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) functions in {{ ydb-short-name }}.

## Create a table and a JSON index


```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);
```


The column type `JsonDocument` stores JSON in a compact binary format and is preferred for an indexed column. Alternatively, the type `Json` (text representation) can be used.

The primary key of the table must consist of a single integer-type column (`Uint64`, `Uint32`, `Int64` or `Int32`) — this is the [current limitation](../../dev/json-indexes.md#limitations) of the JSON index implementation.

## Add test data


```yql
UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"user": {"id": 100, "name": "Alice"}, "active": true}@@)),
    (2, JsonDocument(@@{"user": {"id": 101, "name": "Bob"}, "active": false}@@)),
    (3, JsonDocument(@@{"user": {"id": 102, "name": "Charlie"}, "archived": true}@@));
```


Here the construct `@@...@@` is a [multiline string literal](../../yql/reference/syntax/expressions.md), convenient for writing JSON without escaping quotes. The function `JsonDocument(...)` converts text to a value of type `JsonDocument`.

## Filter by presence of a path in the document

The function [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) checks whether a path specified by a JsonPath expression exists in the document.


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


The path token `$.user.id` is used for index search. The index returns the result without scanning the main table.

## Selecting rows with a specific document field value

The [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) function extracts a scalar value by JsonPath; to use the index you must specify the return type in the `RETURNING` section:


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


When checking equality, the token "path + value" (`$.user.name = "Alice"`) is added to the index, which provides the highest selectivity.

## Combination of conditions

Multiple calls to `JSON_EXISTS` or `JSON_VALUE` on a single indexed JSON column can be combined using the `AND` and `OR` operators:


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
* [INDEX (CREATE TABLE)](../../yql/reference/syntax/create_table/json_index.md) — syntax for creating a JSON index.
* [{#T}](json-index-catalog.md) — an example product catalog with nested attributes.
