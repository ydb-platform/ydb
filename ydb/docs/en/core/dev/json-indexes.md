# JSON indexes

JSON indexes are a type of secondary index implemented on top of the [inverted index](https://en.wikipedia.org/wiki/Inverted_index) that speeds up filtering table rows by conditions applied to the contents of columns of type `Json` and `JsonDocument`. The index is used if the predicate `WHERE` contains the functions [JSON_EXISTS](../yql/reference/builtins/json.md) and [JSON_VALUE](../yql/reference/builtins/json.md) with [JsonPath](../yql/reference/builtins/json.md#jsonpath) expressions. Unlike traditional secondary indexes optimized for equality or range searches on individual table columns, a JSON index works with arbitrary paths inside a JSON document.

For a general description of JSON search and the inverted index on JSON document paths, see the section [{#T}](../concepts/query_execution/json_search.md).

## JSON index characteristics {#characteristics}

JSON indexes in {{ ydb-short-name }} allow:

* Quickly filter rows using [JSON_EXISTS](#json-exists) and [JSON_VALUE](#json-value) with [JsonPath](../yql/reference/builtins/json.md#jsonpath) expressions.
* Combine indexed conditions with operators `AND` and `OR`
* use the values of query parameters passed by the application when processing the evaluated predicates.

JSON index is a [global synchronous](../concepts/glossary.md#secondary-index) index — its data is always consistent with the primary table.

When executing a query, a JSON index may be applied:

- explicitly — via the `<table_name> VIEW <index_name>` operator
- automatically — [optimizer](../concepts/glossary.md#optimizer), if the predicate matches the formal rules.

## JSON index syntax {#syntax}

Creating a JSON index:

* when creating a table: [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md).
* Adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#add-index).

Deletion of JSON indexes is performed via [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#drop-index):


```yql
ALTER TABLE documents DROP INDEX json_idx
```


Query syntax with an explicit JSON index:

* [VIEW (JSON index)](../yql/reference/syntax/select/json_index.md).

Functions and expressions for working with JSON in predicates:

* [Functions for working with JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS` and `JSON_VALUE`
* [JsonPath](../yql/reference/builtins/json.md#jsonpath) — query language for accessing values inside JSON.

Ready-to-use usage scenarios are collected in the [JSON document search recipes](../recipes/json-search/index.md).

## Updating JSON indexes {#update}

JSON indexes are automatically maintained when data is modified and are updated synchronously together with the main table. Tables with JSON indexes support:

* `INSERT`
* `UPSERT`
* `REPLACE`
* `UPDATE`
* `DELETE`

Batch operations (`BATCH UPDATE` and `BATCH DELETE`) are not supported for tables with JSON indexes. If you attempt to execute such a query on a table that has a JSON index, the query will be rejected, the application will receive the corresponding error, and the data will remain unchanged.

Additionally, the following is not supported for tables with JSON indexes:

* Bulk data loading via the `BulkUpsert` call — the requested operation will be rejected with an appropriate error message.
* automatic row deletion by [TTL](../concepts/ttl.md) — errors will be returned when trying to create a table with both a TTL policy and a JSON index, as well as when trying to obtain such a combination of properties via the `ALTER TABLE` commands.

## Supported predicates {#predicates}

Only expressions based on functions `JSON_EXISTS` and `JSON_VALUE` in the `WHERE` block, combined with operators `AND` or `OR`, are supported for execution via JSON indexes according to the rules below.

### JSON_EXISTS {#json-exists}

Checking for the existence of a path or a value within a JsonPath filter.

**Allowed:**


```yql
-- Document root (value is not NULL)
WHERE JSON_EXISTS(doc, '$')

-- Key chain; array indexes are "transparent"
WHERE JSON_EXISTS(doc, '$.user.name')
WHERE JSON_EXISTS(doc, '$.items[*].sku')
WHERE JSON_EXISTS(doc, '$.items[0 to last].active')

-- Filter ? (...) — predicates inside the filter are allowed
WHERE JSON_EXISTS(doc, '$.items ? (@.price == 100)')
WHERE JSON_EXISTS(doc, '$.items ? (@.qty >= 1 && @.qty <= 10)')
WHERE JSON_EXISTS(doc, '$.items ? (@.tag == $t)' PASSING "sale" AS t)

-- JsonPath methods (path is indexed up to the method; exact check is performed by post-filter)
WHERE JSON_EXISTS(doc, '$.value.type()')
WHERE JSON_EXISTS(doc, '$.arr.size()')

-- Combinations on a single column
WHERE JSON_EXISTS(doc, '$.a') AND JSON_EXISTS(doc, '$.b')
WHERE JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b')
```


**Forbidden** (error when using the `VIEW` operator or index auto‑selection refusal):


```yql
-- Comparison predicates at the top level of the path (outside ? (...))
WHERE JSON_EXISTS(doc, '$.key == 10')
WHERE JSON_EXISTS(doc, 'exists($.key)')
WHERE JSON_EXISTS(doc, '$.key starts with "a"')

-- Negation in JsonPath
WHERE JSON_EXISTS(doc, '!($.key == 10)')

-- ON ERROR TRUE
WHERE JSON_EXISTS(doc, '$.key' TRUE ON ERROR)

-- Path without context operator ($) — the supplied document is not used
WHERE JSON_EXISTS(doc, '1')
```


{% note info %}

The function `JSON_EXISTS` returns `true` for any non-empty JsonPath result. The predicate `$.key == 10` specified at the top level would indicate path existence even when the comparison is false, which does not match the expected semantics. Comparisons should be moved into calls to `JSON_VALUE` or into a filter of the form `? (...)`.

{% endnote %}

### JSON_VALUE {#json-value}

Extracting a scalar value with a required `RETURNING <type>`.

When comparing a value via `JSON_VALUE` you always need to specify `RETURNING` with the appropriate type. By default, `JSON_VALUE` returns type `Utf8`, which during query execution leads to an incorrect comparison — values of different types are compared as strings:


```yql
$tmp = Json(@@["1", 1]@@);
SELECT JSON_VALUE($tmp, '$[0]') == "1"; -- true: correct, string is compared with string
SELECT JSON_VALUE($tmp, '$[1]') == "1"; -- true: incorrect, number is compared with string
```


Supported types for the `RETURNING` section: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`.

**Examples of predicates applied via JSON index:**


```yql
-- Equality (path + value go into the index)
WHERE JSON_VALUE(doc, '$.user.age' RETURNING Int32) = 25
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) = true
WHERE JSON_VALUE(doc, '$.name' RETURNING Utf8) = "Alice"u

-- Implicit comparison with true for Bool
WHERE JSON_VALUE(doc, '$.active' RETURNING Bool)

-- Parameters
WHERE JSON_VALUE(doc, '$.user.id' RETURNING Int64) = $id
WHERE JSON_VALUE(doc, '$.tag' RETURNING Utf8) = $tag

-- Comparisons (only the path is used in the index, comparison is performed by post-filter)
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) > 0
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) != 100
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) BETWEEN 1 AND 10
WHERE JSON_VALUE(doc, '$.score' RETURNING Int64) NOT BETWEEN 0 AND 5

-- IN: list of literals
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN ("open"u, "pending"u)

-- IN: supplied parameter of type List<Utf8>
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN $status_list

-- PASSING for JsonPath variables
WHERE JSON_VALUE(doc, '$.x ? (@.y == $v)' RETURNING Int64 PASSING 42 AS v) = 10

-- JsonPath predicates inside the path.
-- Unlike JSON_EXISTS, predicates at the top level are allowed.
WHERE JSON_VALUE(doc, '$.user ? (@.role == "admin")' RETURNING Utf8) = "ok"u
WHERE JSON_VALUE(doc, '$.code starts with "A"' RETURNING String) != ""
WHERE JSON_VALUE(doc, 'exists($.meta)' RETURNING Bool)

-- AND / OR combinations on a single column
WHERE JSON_VALUE(doc, '$.a' RETURNING Int32) = 1
   OR JSON_VALUE(doc, '$.b' RETURNING Int32) = 2
WHERE JSON_EXISTS(doc, '$.a') AND JSON_VALUE(doc, '$.a' RETURNING Int32) = 10
```


**Examples of predicates that cannot be applied via JSON index:**


```yql
-- Calling JSON_VALUE without RETURNING
WHERE JSON_VALUE(doc, '$.key') = "x"

-- DEFAULT with ON EMPTY / ON ERROR (except NULL)
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8 DEFAULT "x" ON ERROR) = "y"

-- Unsupported data type in RETURNING
WHERE JSON_VALUE(doc, '$.ts' RETURNING Timestamp) = ...

-- RETURNING Bool with comparison operators
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) >= true

-- IS NULL / IS NOT NULL — semantically conflict with the "path existence" index
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8) IS NULL

-- Comparison of two JSON_VALUE from different columns
WHERE JSON_VALUE(doc1, '$.k' RETURNING Utf8) = JSON_VALUE(doc2, '$.k' RETURNING Utf8)

-- Nested JSON_* in arguments
WHERE JSON_VALUE(JSON_QUERY(doc, '$.a'), '$.b' RETURNING Utf8) = "x"
```


{% note info %}

To check “value equals `false`” or “value equals `null`”, use a JsonPath filter inside `JSON_EXISTS`, for example `JSON_EXISTS(doc, '$.k ? (@ == false)')` or `JSON_EXISTS(doc, '$.k ? (@ == null)')`, instead of `JSON_VALUE(...) IS NULL`.

{% endnote %}

## Limitations {#limitations}

* JSON indexes are supported only for [string](../concepts/datamodel/table.md#row-oriented-tables) tables.
* The primary key of the table must consist of a single column of integer type (`Uint64`, `Uint32`, `Int64` or `Int32`). This is a temporary limitation that will be removed as development progresses.
* A single JSON index indexes exactly one column of type `Json` or `JsonDocument`.
* The expression `COVER` is not supported for JSON indexes.
* For tables with JSON indexes, a number of operations and data-modification mechanisms [are not supported](#update).
* The type of a read query parameter from the index cannot be wrapped in `Optional<T>` — optional parameters are not supported.
* Equality comparison with an integer literal whose absolute value exceeds 2⁵³ is not accelerated by a value index (such numbers do not fit into the numeric type used in `Json` and `JsonDocument`) and falls back to a path existence check.
* Casting of floating-point literals (`Float`, `Double`) to integer types during comparison is not performed — such comparison is not accelerated by the index.

## Recipes {#recipes}

Ready-to-use scenarios for working with a JSON index:

* [{#T}](../recipes/json-search/json-index-quickstart.md) — quick start.
* [{#T}](../recipes/json-search/json-index-catalog.md) — product catalog with nested attributes.
* [{#T}](../recipes/json-search/json-index-parameters.md) — parameterized queries and JsonPath variables.
* [{#T}](../recipes/json-search/json-index-typecheck.md) — field type and path existence check.

## Related materials {#see-also}

- [Functions for working with JSON](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, JsonPath syntax.
- [Secondary indexes](secondary-indexes.md) — general information about global indexes and `VIEW`.
- [Full-text indexes](fulltext-indexes.md) — a related mechanism built on top of an inverted index for words and phrases.
- [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md) and [VIEW (JSON index)](../yql/reference/syntax/select/json_index.md) — syntax reference.
