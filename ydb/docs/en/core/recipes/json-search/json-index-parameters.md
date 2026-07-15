# Parameterized queries and JsonPath variables

In most applications, query input data is not substituted into SQL text but passed through [parameters](../../yql/reference/syntax/declare.md). Typical use cases for working with [JSON indexes](../../dev/json-indexes.md) include:

1. direct comparison of the `JSON_VALUE` result with a parameter;
2. checking if the result is in a list of values using the `IN` expression;
3. passing a parameter to a JsonPath expression via the `PASSING` clause.

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

The most common case is when a parameter is substituted as the right operand of a comparison, and the left operand is a `JSON_VALUE` call with an explicit `RETURNING`:


```yql
DECLARE $owner_id AS Int64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $owner_id
  AND JSON_EXISTS(payload, '$.archived ? (@ == false)');
```


The index contains the "path + value" token (`$.owner_id = $owner_id`) — the parameter is treated as a regular value. This allows you to perform a lookup with the same selectivity as when comparing with a literal.

Running with `$owner_id = 100` returns rows `1` and `2`.

## Search by a list of values

To search by multiple values of a single field, it is convenient to use `IN`:


```yql
DECLARE $tags AS List<Utf8>;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
```


Index behavior differs depending on whether it is a literal or a parameter:

* `IN ("active"u, "pending"u)` — a list of literals is converted into `OR` of several "path + value" tokens, one for each list value.
* `IN $tags` — for each parameter value during query execution, a "path + value" token is formed (`$.tag` + value from the list). At compile time, the parameter values are not yet known, so in the query plan (`EXPLAIN`) such a condition is displayed as a "path + parameter" pair (`{"path": "$.tag", "param": "$tags"}`).

The parameter for `IN` can be any collection of scalar values: `List<T>`, `Tuple<T, ...>`, `Dict<K, V>`, or `Set<T>`.

Running with `$tags = ["active"u, "pending"u]` returns rows `1`, `3`, and `4`.

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


The path token `$.content.y` is used for index lookup, and the `@.y > $threshold` condition is checked by a post-filter.

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

All three methods support parameters with the following types: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`. Optional types (`Optional<T>`) are not supported in parameters.

For more details about types, see [{#T}](../../dev/json-indexes.md#json-value).

## More details

* [{#T}](json-index-quickstart.md) — basic use case for a JSON index.
* [Passing parameters to JSON index predicates](../../dev/json-indexes.md#json-value) — detailed description of all options.
* [JsonPath](../../yql/reference/builtins/json.md#jsonpath) — syntax of the JsonPath language.
