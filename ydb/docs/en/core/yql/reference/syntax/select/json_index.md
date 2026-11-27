# VIEW (JSON index)

To execute a `SELECT` query on a row table with explicit use of a [JSON index](../../../../dev/json-indexes.md), use the `VIEW` expression:

```yql
SELECT ...
FROM documents VIEW json_idx
WHERE <predicate based on JSON_EXISTS / JSON_VALUE>
ORDER BY ...
```

In the query example above, `documents` is the name of the table containing a column of type `Json` or `JsonDocument`, and `json_idx` is the name of the JSON index created on that column.

If the predicate is not supported for execution via a JSON index, a query with an explicit `VIEW` expression fails with a compile-time error. Without the `VIEW` expression, the optimizer cannot select this index for such a predicate, and the query will be executed by another method (for example, selecting a different index or performing a full scan of the base table).

{% note info %}

A JSON index can be selected automatically by the [optimizer](../../../../concepts/glossary.md#optimizer) if the predicate meets the requirements for index usage. For debugging and guaranteed index usage, specify it explicitly using `VIEW IndexName`.

For a description of supported predicates, see the [{#T}](../../../../dev/json-indexes.md#predicates) section.

{% endnote %}

## Automatic index selection {#auto}

If the `WHERE` predicate contains `JSON_EXISTS` / `JSON_VALUE` calls on an indexed JSON column, the [optimizer](../../../../concepts/glossary.md#optimizer) may use the JSON index without an explicit `VIEW`. Automatic selection follows these rules:

* The JSON index is considered by the optimizer with the lowest priority — it is a fallback option that is selected only after other access methods have been ruled out.
* The JSON index is not selected if the query can already be served by a [primary key](../../../../concepts/glossary.md#primary-key) or another, more specific [secondary index](../../../../concepts/glossary.md#secondary-index).
* Explicit specification of `VIEW` overrides the optimizer's decision and forces index usage.
* All indexed subexpressions in a single query must refer to the same indexed JSON column. If predicates on different indexed columns are combined via `AND` / `OR`, automatic selection is not applied.

{% note info %}

Automatic JSON index selection is rule-based: it uses the query structure and schema metadata, without data statistics. The selection logic may change in future versions of {{ ydb-short-name }}, so for guaranteed index usage, specify it explicitly via `VIEW`.

{% endnote %}

## Searching with filter columns {#filtered}

For an index [created](../create_table/json_index.md#filtered) with `ON (user_id, payload)`, specify an equality condition on `user_id` together with the JSON condition:

```yql
DECLARE $user_id AS Uint64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE user_id = $user_id AND JSON_EXISTS(payload, '$.kind');
```

If the index has several filter columns, an equality condition is required for each of them. Without these conditions, a query with an explicit `VIEW` fails, and automatic selection does not use this index.

## JSON_EXISTS

[JSON_EXISTS(doc, jsonpath)](../../builtins/json.md#json_exists) checks for the existence of a path or value inside a JSON document:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id');
```

The JSON index supports almost all JsonPath syntax, except for nested predicates and boolean expressions at the context object level (`$`) in `JSON_EXISTS`. A more complex example of a supported expression:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user ? (@.id > 100)');
```

## JSON_VALUE

[JSON_VALUE(doc, jsonpath RETURNING <type>)](../../builtins/json.md#json_value) extracts a scalar value by a JsonPath and returns it in the specified type. To use the JSON index, the `RETURNING <type>` clause is required:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u;
```

When checking equality conditions or whether a value is in a given list (`IN`), the index search is performed by the 'path + value' token, which provides the highest selectivity. For other comparisons (`!=`, `<`, `>=`, `BETWEEN`, etc.), only the path token is used, and the final comparison accuracy is ensured by a post-filter.

The indexed predicates do not support `ERROR ON ERROR` in `JSON_EXISTS`, or `ERROR ON EMPTY` and `ERROR ON ERROR` in `JSON_VALUE`; for more details, see [supported predicates](../../../../dev/json-indexes.md#predicates).

## Parameters

Three ways to pass query parameters are supported:

1. Direct comparison of the `JSON_VALUE` result with a parameter:

    ```yql
    DECLARE $id AS Int64;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $id;
    ```

2. Checking whether the result is in a list of values:

    ```yql
    DECLARE $tags AS List<Utf8>;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
    ```

3. Passing a parameter to JsonPath via the `PASSING` clause:

    ```yql
    DECLARE $v AS Int64;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_EXISTS(payload, '$.k ? (@ == $v)' PASSING $v AS v);
    ```

### JSON parameters {#json-parameters}

`PASSING` also accepts parameters of type `Json`. For example, find documents whose `tags` array contains at least one of the supplied values:

```yql
DECLARE $values AS Json;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.tags[*] ? (@ == $values)' PASSING $values AS values);
```

With `$values` containing the JSON array `["red", "large"]`, the query finds documents tagged `red` or `large`. In `lax` mode (the default), equality checks for a match with any array element. Repeated values do not produce duplicate rows, and an empty array returns no rows. The full JsonPath condition is checked after the index lookup.

## AND and OR combinations

`JSON_EXISTS` and `JSON_VALUE` on the same JSON column can be combined in a single `WHERE` using the `AND` and `OR` operators:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u
  AND JSON_VALUE(payload, '$.user.id' RETURNING Int64) BETWEEN 100 AND 200;
```

{% note info %}

Query execution via a JSON index can only use a subset of expressions based on `JSON_EXISTS` and `JSON_VALUE` (see [{#T}](../../../../dev/json-indexes.md#predicates)). Conditions that do not fall under these rules (for example, negations, comparisons with another column, comparison of two `JSON_VALUE` from different columns) are not indexed; in `AND` they go into a post-filter, in `OR` they lead to rejection of index usage for the entire group of conditions combined via `OR`.

{% endnote %}

## Error handling {#error-handling}

Conditions with `ERROR ON EMPTY` or `ERROR ON ERROR` cannot be used for index lookup: it could skip rows that should raise an error. Such a condition can still be checked after lookup if another condition joined by `AND` uses the index. Otherwise, a query with an explicit `VIEW` fails, and automatic selection does not use this index.
