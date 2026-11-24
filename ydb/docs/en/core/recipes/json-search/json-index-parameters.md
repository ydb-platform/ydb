# Parameterized queries and JsonPath variables

In most applications, query input data is not substituted into SQL text but is passed via [parameters](../../yql/reference/syntax/declare.md). Typical use cases for parameters when working with [JSON indexes](../../dev/json-indexes.md):

1. direct comparison of the result of `JSON_VALUE` with a parameter;
2. checking whether the result is in a list of values using the `IN` expression;
3. passing a parameter to a JsonPath expression via the `PASSING` clause;
4. finding the intersection of JSON arrays using a parameter of type `Json`.

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
        "tags": ["red", "large"],
        "archived": false,
        "content": {"x": 1, "y": 1}
    }@@)),
    (2, JsonDocument(@@{
        "owner_id": 100,
        "tag": "draft",
        "tags": ["blue"],
        "archived": false,
        "content": {"x": 1, "y": 2}
    }@@)),
    (3, JsonDocument(@@{
        "owner_id": 101,
        "tag": "active",
        "tags": ["green", "large"],
        "archived": true,
        "content": {"x": 2, "y": 1}
    }@@)),
    (4, JsonDocument(@@{
        "owner_id": 102,
        "tag": "pending",
        "tags": [],
        "archived": false,
        "content": {"x": 2, "y": 2}
    }@@));
```

## Direct comparison with a parameter

The most common case is that the parameter is substituted as the right operand of the comparison, and the left operand is a call to `JSON_VALUE` with an explicit `RETURNING`:

```yql
DECLARE $owner_id AS Int64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $owner_id
  AND JSON_EXISTS(payload, '$.archived ? (@ == false)');
```

The index receives a token «path + value» (`$.owner_id = $owner_id`) — the parameter is treated as a regular value. This allows performing a query with the same selectivity as when comparing with a literal.

Running with `$owner_id = 100` returns rows `1` and `2`.

## Search by a list of values

To search by multiple values of a single field, you can use `IN`:

```yql
DECLARE $tags AS List<Utf8>;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
```

The behavior of the index differs depending on whether it is a literal or a parameter:

* `IN ("active"u, "pending"u)` — a list of literals is converted into `OR` of several «path + value» tokens, one for each value in the list.
* `IN $tags` — for each parameter value during query execution, a «path + value» token is formed (`$.tag` + value from the list). At the compilation stage, the parameter values are not yet known, so in the query plan (`EXPLAIN`) such a condition is displayed as a «path + parameter» pair (`{"path": "$.tag", "param": "$tags"}`).

Any collection of scalar values can serve as a parameter for `IN`: `List<T>`, `Tuple<T, ...>`, `Dict<K, V>`, or `Set<T>`.

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

Indexed predicates do not support `ERROR ON ERROR` in `JSON_EXISTS`, or `ERROR ON EMPTY` and `ERROR ON ERROR` in `JSON_VALUE`. See [Error handling](../../yql/reference/syntax/select/json_index.md#error-handling).

## Intersecting JSON arrays

For documents `1: ["red", "large"]`, `2: ["blue"]`, `3: ["green", "large"]`, and `4: []`, one query accepts arrays of different lengths:

```yql
DECLARE $values AS Json;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(
    payload,
    '$.tags[*] ? (@ == $values)'
    PASSING $values AS values
)
ORDER BY id;
```

With `$values = ["red", "large"]`, the equality comparison `@ == $values` in the default `lax` mode checks for at least one matching tag. The index combines matches for `"red"` and `"large"` using `OR`, and a post-filter checks the complete JsonPath condition. The query returns rows `1` and `3`. With `$values = []`, the query returns no rows.

## Supported parameter types

Supported scalar parameter types are `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), and `Bool`. `IN` accepts collections of these values. `PASSING` also accepts `Json` parameters for equality comparisons, including arrays of values. Optional types (`Optional<T>`) are not supported in parameters.

For more details about types, see [{#T}](../../dev/json-indexes.md#json-value).

## See also

* [{#T}](json-index-quickstart.md) — basic use case for a JSON index.
* [Passing parameters to JSON index predicates](../../dev/json-indexes.md#json-value) — detailed description of all options.
* [JsonPath](../../yql/reference/builtins/json.md#jsonpath) — JsonPath language syntax.
