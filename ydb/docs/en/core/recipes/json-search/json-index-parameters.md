# Parameterized queries and JsonPath variables

In most applications, query input data is not substituted into SQL text but is passed via [parameters](../../yql/reference/syntax/declare.md). Typical use cases for parameters when working with [JSON indexes](../../dev/json-indexes.md):

1. direct comparison of the result of `JSON_VALUE` with a parameter
2. checking whether the result is in a list of values using the `IN` expression
3. passing a parameter to a JsonPath expression via the `PASSING` clause.

## Prerequisites

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

The most common case is that the parameter is substituted as the right operand of the comparison, and the left operand is the `JSON_VALUE` call with an explicit `RETURNING`:


```yql
DECLARE $owner_id AS Int64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $owner_id
  AND JSON_EXISTS(payload, '$.archived ? (@ == false)');
```


The index receives a token «path + value» (`$.owner_id = $owner_id`) — the parameter is treated as a regular value. This allows performing a query with the same selectivity as when comparing with a literal.

Running with `$owner_id = 100` will return rows `1` and `2`.

## Search by a list of values

To search by multiple values of a single field, you can use `IN`:


```yql
DECLARE $tags AS List<Utf8>;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
```


The behavior of the index differs depending on whether it is a literal or a parameter:

* `IN ("active"u, "pending"u)` — a list of literals turns into `OR` of several «path + value» tokens, one for each value in the list.
* `IN $tags` — for each parameter value during query execution, a «path + value» token is formed (`$.tag` + value from the list). At compile time, the parameter values are not yet known, so in the query plan (`EXPLAIN`) such a condition is displayed as a «path + parameter» pair (`{"path": "$.tag", "param": "$tags"}`).

Any collection of scalar values can be a parameter for `IN`: `List<T>`, `Tuple<T, ...>`, `Dict<K, V>`, or `Set<T>`.

Running with `$tags = ["active"u, "pending"u]` will return rows `1`, `3`, and `4`.

## Parameters inside JsonPath (PASSING)

If a parameter must be used inside a JsonPath filter (`? (...)`), it is passed in the `PASSING` clause:


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


For index search, the path token `$.content.y` is used, and the condition `@.y > $threshold` is checked by a post-filter.

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

For all three methods, parameters with the following types are supported: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`. Optional types (`Optional<T>`) are not supported in parameters.

For more information about types, see [{#T}](../../dev/json-indexes.md#json-value).

## Learn more

* [{#T}](json-index-quickstart.md) — basic usage scenario for a JSON index.
* [Passing parameters to JSON index predicates](../../dev/json-indexes.md#json-value) — detailed description of all options.
* [JsonPath](../../yql/reference/builtins/json.md#jsonpath) — JsonPath language syntax.
