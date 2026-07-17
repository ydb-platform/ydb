# Checking field type and path existence

This recipe shows how a [JSON index](../../dev/json-indexes.md) is used to check document structure: the presence of a specific path, the type of value at the path, etc. These tasks are common when working with heterogeneous JSON documents, where some fields are optional or may have different types.

## Preparation


```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);

UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"archived": false, "value": 1,    "data": [1, 2, 3]}@@)),
    (2, JsonDocument(@@{"archived": false, "value": 2,    "data": "plain text"}@@)),
    (3, JsonDocument(@@{"archived": true,  "value": 3,    "data": {"nested": true}}@@)),
    (4, JsonDocument(@@{"archived": false, "value": null, "meta": "no data field"}@@));
```


## Find documents where a field contains an array

The JsonPath method `.type()` returns a string name of the value type at the specified path. This allows filtering documents by content type:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u;
```


The path token `$.data` is added to the index — the `.type()` method completes the path token construction. The exact check of the string value `"array"` is performed by a post-filter.

Result:


```text
id
1
```


The full list of values returned by the `.type()` method is given in the [JsonPath](../../yql/reference/builtins/json.md#jsonpath) syntax description.

## Find documents where a field is a non-empty array

The `.size()` method returns the number of array elements (or 1 for a scalar, 0 for a missing path). In combination with a type filter:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u
  AND JSON_VALUE(payload, '$.data.size()' RETURNING Int64) > 0;
```


Both fragments are indexed by the corresponding paths, and the exact values are checked by a post-filter. Result: `id = 1`.

## Find documents with value `false` or `null`

To check "value equals `false`" or "value equals `null`", use JsonPath inside `JSON_EXISTS`, not `JSON_VALUE(...) IS NULL`:


```yql
-- The value of the 'archived' field is false
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.archived ? (@ == false)');

-- The value of the 'value' field is null
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.value ? (@ == null)');
```


The path `$.archived` (or `$.value`) and the values `false` or `null`, respectively, are included in the index search operation.

{% note info %}

The above conditions select documents where the specified attribute is explicitly set to false or null. Checking for the absence of an attribute cannot be performed using a JSON index.

{% endnote %}

## Learn more

* [JSON_EXISTS](../../dev/json-indexes.md#json-exists) — what is allowed in JsonPath expressions.
* [JSON_VALUE](../../dev/json-indexes.md#json-value) — what is allowed when extracting values.
* [JsonPath: methods](../../yql/reference/builtins/json.md#jsonpath) — list of methods (`type`, `size`, `keyvalue`, ...) and JsonPath predicates.
