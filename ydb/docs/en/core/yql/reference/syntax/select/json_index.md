# VIEW (JSON index)

To execute the `SELECT` query on a string table with explicit use of the [JSON index](../../../../dev/json-indexes.md), use the `VIEW` expression:


```yql
SELECT ...
FROM documents VIEW json_idx
WHERE <предикат на основе JSON_EXISTS / JSON_VALUE>
ORDER BY ...
```


In the query example above, `documents` is the name of the table containing a column of type `Json` or `JsonDocument`, and `json_idx` is the name of the JSON index created on that column.

If a predicate is not supported for execution via a JSON index, a query with an explicit `VIEW` expression fails at compile time. Without the `VIEW` expression, the optimizer cannot choose this index for such a predicate, and the query will be executed by another method (for example, using a different index or performing a full scan of the primary table).

{% note info %}

A JSON index can be chosen automatically by the [optimizer](../../../../concepts/glossary.md#optimizer) provided the predicate meets the requirements for index usage. For debugging and guaranteed index usage, specify it explicitly using `VIEW IndexName`.

See the description of supported predicates in the [{#T}](../../../../dev/json-indexes.md#predicates) section.

{% endnote %}

## Automatic index selection {#auto}

If the predicate `WHERE` contains calls to `JSON_EXISTS` or `JSON_VALUE` on an indexed JSON column, the [optimizer](../../../../concepts/glossary.md#optimizer) may use the JSON index without an explicit `VIEW`. Automatic selection follows these rules:

* The JSON index is considered by the optimizer with the lowest priority—it is a fallback option selected only after all other access methods have been excluded.
* The JSON index is not chosen if the query can already be served by the [primary key](../../../../concepts/glossary.md#primary-index) or another, more specific [secondary index](../../../../concepts/glossary.md#secondary-index).
* Explicitly specifying `VIEW` overrides the optimizer’s decision and forces the use of the index.
* All indexed sub‑expressions in a single query must refer to the same indexed JSON column. If you combine predicates on different indexed columns via `AND` or `OR`, automatic selection does not apply.

{% note info %}

Automatic JSON index selection works on a rule‑based system: it uses the query structure and schema metadata, without data statistics. The selection logic may change in future versions {{ ydb-short-name }}, so for guaranteed index usage specify it explicitly via `VIEW`.

{% endnote %}

## JSON_EXISTS

[JSON_EXISTS(doc, jsonpath)](../../builtins/json.md#json_exists) checks for the existence of a path or value inside a JSON document:


```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id');
```


The JSON index supports virtually the entire JsonPath syntax, except for nested predicates and boolean expressions at the context object level (`$`) in `JSON_EXISTS`. A more complex example of a supported expression:


```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user ? (@.id > 100)');
```


## JSON_VALUE

[JSON_VALUE(doc, jsonpath RETURNING <type>)](../../builtins/json.md#json_value) extracts a scalar value by a JsonPath and returns it in the specified type. The `RETURNING <type>` section is required to use the JSON index:


```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u;
```


When checking equality conditions or membership of a value in a given list (`IN`), the index search is performed using the “path + value” token, providing the highest selectivity. For other comparisons (`!=`, `<`, `>=`, `BETWEEN`, etc.), only the path token is used, and the final comparison accuracy is ensured by a post‑filter.

### Parameters

Three methods of passing query parameters are supported:

1. Direct comparison of the `JSON_VALUE` result with a parameter:


   ```yql
   DECLARE $id AS Int64;
   SELECT * FROM documents VIEW json_idx
   WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $id;
   ```

2. Checking for the presence of the result in a list of values:


   ```yql
   DECLARE $tags AS List<Utf8>;
   SELECT * FROM documents VIEW json_idx
   WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
   ```

3. Passing a parameter to JsonPath via the `PASSING` section:


   ```yql
   DECLARE $v AS Int64;
   SELECT * FROM documents VIEW json_idx
   WHERE JSON_EXISTS(payload, '$.k ? (@ == $v)' PASSING $v AS v);
   ```

## AND and OR combinations

`JSON_EXISTS` and `JSON_VALUE` on the same JSON column can be combined in a single `WHERE` using the `AND` and `OR` operators:


```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u
  AND JSON_VALUE(payload, '$.user.id' RETURNING Int64) BETWEEN 100 AND 200;
```


{% note info %}

Selection via a JSON index can use only a subset of expressions based on `JSON_EXISTS` and `JSON_VALUE` (see [{#T}](../../../../dev/json-indexes.md#predicates)). Conditions that do not fall under these rules (e.g., negations, comparisons with another column, comparison of two `JSON_VALUE` from different columns) are not indexed; in `AND` they become a post‑filter, and in `OR` they cause the index to be dropped for the entire group of conditions combined via `OR`.

{% endnote %}
