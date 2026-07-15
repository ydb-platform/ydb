# Check field type and path presence

This recipe shows how the [JSON index](../../dev/json-indexes.md) is used to validate document structure: presence of a specific path, the type of the value at the path, etc. These tasks are common when working with heterogeneous JSON documents, where some fields are optional or may have different types.

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


## Find documents where the field contains an array

The JsonPath method `.type()` returns the string name of the value type at the specified path. This allows you to filter documents by content type:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u;
```


A path token `$.data` is indexed — the method `.type()` completes the construction of the path token. Exact verification of the string value `"array"` is performed by a post-filter.

Result:


```text
id
1
```


The full list of values returned by the method `.type()` is provided in the [JsonPath](../../yql/reference/builtins/json.md#jsonpath) syntax description.

## Find documents where the field is a non‑empty array

The method `.size()` returns the number of array elements (or 1 for a scalar, 0 for a missing path). Combined with a type filter:


```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u
  AND JSON_VALUE(payload, '$.data.size()' RETURNING Int64) > 0;
```


Both fragments are indexed by their respective paths, and exact values are verified by a post-filter. Result: `id = 1`.

## Find documents with value `false` or `null`

To check that the value equals `false` or the value equals `null`, use JsonPath inside `JSON_EXISTS` rather than `JSON_VALUE(...) IS NULL`:


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


The index search operation receives the path `$.archived` (or `$.value`) and the values `false` or `null`, respectively.

{% note info %}

The conditions above select documents where the specified attribute is explicitly set to false or null. You cannot check for the absence of an attribute using a JSON index.

{% endnote %}

## More details

* [JSON_EXISTS](../../dev/json-indexes.md#json-exists) — what is allowed in JsonPath expressions.
* [JSON_VALUE](../../dev/json-indexes.md#json-value) — what is allowed when extracting values.
* [JsonPath: methods](../../yql/reference/builtins/json.md#jsonpath) — list of methods (`type`, `size`, `keyvalue`, ...) and JsonPath predicates.
