# Parameterized queries and JsonPath variables

In most applications, query input data are not substituted into the SQL text, but are passed via [parameters](../../yql/reference/syntax/declare.md). Typical use cases of parameters for working with [JSON indexes](../../dev/json-indexes.md):

1. Direct comparison of the result `JSON_VALUE` with a parameter.
2. Checking for the presence of the result in a list of values via the expression `IN`.
3. Passing a parameter to a JsonPath expression via the `PASSING` section.

## Preparation

Below is an example of a table, a JSON index, and data population.


```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);

UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{
        "owner_id": 100,
        "tag": "active",
        "archived": false,
        "content": {"x": 1, "y": 1}
    }@@)),
    (2, JsonDocument(@@{
        "owner_id": 100,
        "tag": "draft",
        "archived": false,
        "content": {"x": 1, "y": 2}
    }@@)),
    (3, JsonDocument(@@{
        "owner_id": 101,
        "tag": "active",
        "archived": true,
        "content": {"x": 2, "y": 1}
    }@@)),
    (4, JsonDocument(@@{
        "owner_id": 102,
        "tag": "pending",
        "archived": false,
        "content": {"x": 2, "y": 2}
    }@@));
```


## Direct comparison with a parameter

The most common case is that the parameter is substituted as the right operand of the comparison, while the left operand is a call to `JSON_VALUE` with an explicit `RETURNING`:


```yql
DECLARE $owner_id AS Int64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $owner_id
  AND JSON_EXISTS(payload, '$.archived ? (@ == false)');
```


A “path + value” token (`$.owner_id = $owner_id`) is indexed — the parameter is treated as a regular value. This allows selection with the same selectivity as comparing with a literal.

Running with `$owner_id = 100` returns rows `1` and `2`.

## Search over a list of values

To search for multiple values of a single field, it is convenient to use `IN`:


```yql
DECLARE $tags AS List<Utf8>;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
```


The index behavior differs depending on whether it is a literal or a parameter:

* `IN ("active"u, "pending"u)` — a list of literals is turned into `OR` multiple “path + value” tokens, one for each list value.
* `IN $tags` — for each parameter value at query execution time, a “path + value” token (`$.tag` + value from the list) is generated. Since the parameter values are unknown at compilation, the query plan (`EXPLAIN`) represents this condition as a “path + parameter” pair (`{"path": "$.tag", "param": "$tags"}`).

Any collection of scalar values can be used as a parameter for `IN`: `List<T>`, `Tuple<T, ...>`, `Dict<K, V>`, or `Set<T>`.

Running with `$tags = ["active"u, "pending"u]` returns rows `1`, `3`, and `4`.

## Parameters inside JsonPath (PASSING)

If a parameter must be used inside a JsonPath filter (`? (...)`), it is passed in the `PASSING` section:


```yql
DECLARE $min_stock AS Int64;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(
    payload,
    '$.content ? (@.y > $threshold)'
    PASSING $min_stock AS threshold
);
```


The index search uses the path token `$.content.y`, and the condition `@.y > $threshold` is verified by a post-filter.

Similarly, `PASSING` works in `JSON_VALUE`:


```yql
DECLARE $v AS Int64;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(
    payload,
    '$.content ? (@.y == $val)'
    PASSING $v AS val
    RETURNING Int64
) = 10;
```


## Supported parameter types

For all three methods, parameters of the following types are supported: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`. Optional types (`Optional<T>`) are not supported in parameters.

For more information about types, see [{#T}](../../dev/json-indexes.md#json-value).

## See also

* [{#T}](json-index-quickstart.md) — basic JSON index usage scenario.
* [Passing parameters to JSON index predicates](../../dev/json-indexes.md#json-value) — detailed description of all options.
* [JsonPath](../../yql/reference/builtins/json.md#jsonpath) — JsonPath language syntax.
