# JSON indexes

JSON indexes are a type of secondary index implemented on top of an [inverted index](https://en.wikipedia.org/wiki/Inverted_index) that accelerates filtering table rows by conditions imposed on the contents of columns of type `Json` and `JsonDocument`. The index is used if the predicate `WHERE` uses the functions [JSON_EXISTS](../yql/reference/builtins/json.md) and [JSON_VALUE](../yql/reference/builtins/json.md) with [JsonPath](../yql/reference/builtins/json.md#jsonpath) expressions. Unlike traditional secondary indexes optimized for equality or range searches on individual table columns, a JSON index works with arbitrary paths within a JSON document.

For a general description of JSON search and the structure of an inverted index on JSON document paths, see the [{#T}](../concepts/query_execution/json_search.md) section.

## Characteristics of JSON indexes {#characteristics}

JSON indexes in {{ ydb-short-name }} allow:

* Quickly filter rows by [JSON_EXISTS](#json-exists) and [JSON_VALUE](#json-value) with [JsonPath](../yql/reference/builtins/json.md#jsonpath) expressions
* Combine indexed conditions using operators `AND` and `OR`.
* use query parameter values passed by the application when processing checked predicates.

JSON index is a [global synchronous](../concepts/glossary.md#secondary-index) index — its data is always consistent with the main table.

When executing a query, a JSON index can be applied:

- Explicitly — via the `<table_name> VIEW <index_name>` operator
- automatically — [optimizer](../concepts/glossary.md#optimizer), if the predicate matches the formal rules.

## JSON index syntax {#syntax}

Creating a JSON index:

* when creating a table: [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md)
* Adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#add-index).

JSON indexes are deleted via [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#drop-index):


```yql
ALTER TABLE documents DROP INDEX json_idx
```


Query syntax with explicit JSON index:

* [VIEW (JSON index)](../yql/reference/syntax/select/json_index.md).

Functions and expressions for working with JSON in predicates:

* [JSON functions](../yql/reference/builtins/json.md) — `JSON_EXISTS` and `JSON_VALUE`
* [JsonPath](../yql/reference/builtins/json.md#jsonpath) is a query language for accessing values inside JSON.

Ready-made use cases are collected in the [JSON document search recipes](../recipes/json-search/index.md) section.

## Updating JSON indexes {#update}

JSON indexes are automatically maintained when data is modified and are updated synchronously with the main table. Tables with JSON indexes support:

* `INSERT`
* `UPSERT`
* `REPLACE`
* `UPDATE`
* `DELETE`

Batch operations (`BATCH UPDATE` and `BATCH DELETE`) are not supported for tables with JSON indexes. If you attempt to execute such a query on a table for which a JSON index has been created, the query will be rejected, a corresponding error will be returned to the application, and the data will remain unchanged.

In addition, the following is not supported for tables with JSON indexes:

* mass data loading via the `BulkUpsert` call — the requested operation will be rejected with a corresponding error message
* Automatic deletion of rows by [TTL](../concepts/ttl.md) — errors are returned when trying to create a table with both a TTL policy and a JSON index, as well as when trying to get such a combination of properties through `ALTER TABLE` commands.

## Supported predicates {#predicates}

For execution via JSON indexes, only expressions based on the `JSON_EXISTS` and `JSON_VALUE` functions in the `WHERE` block, combined by the `AND` or `OR` operators according to the rules below.

### JSON_EXISTS {#json-exists}

Checking the existence of a path or value inside a JsonPath filter.

**Allowed:**


```yql
-- Document root (value not NULL)
WHERE JSON_EXISTS(doc, '$')

-- Key chain; array indexes are 'transparent'
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

-- Combinations on one column
WHERE JSON_EXISTS(doc, '$.a') AND JSON_EXISTS(doc, '$.b')
WHERE JSON_EXISTS(doc, '$.a') OR JSON_EXISTS(doc, '$.b')
```


**Forbidden** (error when using the `VIEW` operator or auto-index selection failure):


```yql
-- Comparison predicates at the top level of the path (outside ? (...))
WHERE JSON_EXISTS(doc, '$.key == 10')
WHERE JSON_EXISTS(doc, 'exists($.key)')
WHERE JSON_EXISTS(doc, '$.key starts with "a"')

-- Negation in JsonPath
WHERE JSON_EXISTS(doc, '!($.key == 10)')

-- ON ERROR TRUE
WHERE JSON_EXISTS(doc, '$.key' TRUE ON ERROR)

-- Path without context operator ($) — the passed document is not used
WHERE JSON_EXISTS(doc, '1')
```


{% note info %}

The `JSON_EXISTS` function returns `true` for any non-empty JsonPath result. The `$.key == 10` predicate specified at the top level would give "path existence" even when the comparison is false, which does not match the expected semantics. Comparisons should be moved into `JSON_VALUE` calls or into a filter of the form `? (...)`.

{% endnote %}

### JSON_VALUE {#json-value}

Extracting a scalar value with a required `RETURNING <type>`.

To compare a value using `JSON_VALUE`, you must always specify `RETURNING` with the required type. By default, `JSON_VALUE` returns type `Utf8`, which leads to incorrect comparison during query execution — values of different types are compared as strings:


```yql
$tmp = Json(@@["1", 1]@@);
SELECT JSON_VALUE($tmp, '$[0]') == "1"; -- true: correct, string compared with string
SELECT JSON_VALUE($tmp, '$[1]') == "1"; -- true: incorrect, number compared with string
```


Supported types for the `RETURNING` section: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`.

**Examples of predicates applied via a JSON index:**


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

-- IN: specified parameter of type List<Utf8>
WHERE JSON_VALUE(doc, '$.status' RETURNING Utf8) IN $status_list

-- PASSING for JsonPath variables
WHERE JSON_VALUE(doc, '$.x ? (@.y == $v)' RETURNING Int64 PASSING 42 AS v) = 10

-- JsonPath predicates inside the path.
-- Unlike JSON_EXISTS, predicates at the top level are allowed.
WHERE JSON_VALUE(doc, '$.user ? (@.role == "admin")' RETURNING Utf8) = "ok"u
WHERE JSON_VALUE(doc, '$.code starts with "A"' RETURNING String) != ""
WHERE JSON_VALUE(doc, 'exists($.meta)' RETURNING Bool)

-- AND / OR combinations on one column
WHERE JSON_VALUE(doc, '$.a' RETURNING Int32) = 1
   OR JSON_VALUE(doc, '$.b' RETURNING Int32) = 2
WHERE JSON_EXISTS(doc, '$.a') AND JSON_VALUE(doc, '$.a' RETURNING Int32) = 10
```


**Examples of predicates that cannot be applied via a JSON index:**


```yql
-- JSON_VALUE call without RETURNING
WHERE JSON_VALUE(doc, '$.key') = "x"

-- DEFAULT on ON EMPTY / ON ERROR (except NULL)
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8 DEFAULT "x" ON ERROR) = "y"

-- Unsupported data type in RETURNING
WHERE JSON_VALUE(doc, '$.ts' RETURNING Timestamp) = ...

-- RETURNING Bool with comparison operators
WHERE JSON_VALUE(doc, '$.flag' RETURNING Bool) >= true

-- IS NULL / IS NOT NULL — semantically contradict the 'path existence' index
WHERE JSON_VALUE(doc, '$.k' RETURNING Utf8) IS NULL

-- Comparison of two JSON_VALUE from different columns
WHERE JSON_VALUE(doc1, '$.k' RETURNING Utf8) = JSON_VALUE(doc2, '$.k' RETURNING Utf8)

-- Nested JSON_* in arguments
WHERE JSON_VALUE(JSON_QUERY(doc, '$.a'), '$.b' RETURNING Utf8) = "x"
```


{% note info %}

To check "value equals `false`" or "value equals `null`", use a JsonPath filter inside `JSON_EXISTS`, for example `JSON_EXISTS(doc, '$.k ? (@ == false)')` or `JSON_EXISTS(doc, '$.k ? (@ == null)')`, not `JSON_VALUE(...) IS NULL`.

{% endnote %}

## Limitations {#limitations}

* JSON indexes are supported only for [row](../concepts/datamodel/table.md#row-oriented-tables) tables.
* The table's primary key must consist of a single column of an integer type (`Uint64`, `Uint32`, `Int64`, or `Int32`). This is a temporary limitation that will be removed in future development.
* A single JSON index indexes exactly one column of type `Json` or `JsonDocument`.
* The `COVER` expression is not supported for JSON indexes.
* A number of data modification operations and mechanisms are [not supported](#update) for tables with JSON indexes.
* The type of the index read query parameter cannot be wrapped in `Optional<T>` — optional parameters are not supported.
* Equality comparison with an integer literal whose absolute value exceeds 2⁵³ is not accelerated by a value index (such numbers do not fit into the numeric type used in `Json` and `JsonDocument`) and is reduced to checking the existence of the path.
* Casting floating-point literals (`Float`, `Double`) to integer types during comparison is not performed — such comparison is not accelerated by the index.

## Recipes {#recipes}

Ready-made scenarios for working with a JSON index:

* [{#T}](../recipes/json-search/json-index-quickstart.md) — quick start.
* [{#T}](../recipes/json-search/json-index-catalog.md) — product catalog with nested attributes.
* [{#T}](../recipes/json-search/json-index-parameters.md) — parameterized queries and JsonPath variables.
* [{#T}](../recipes/json-search/json-index-typecheck.md) — checking field type and path existence.

## Related materials {#see-also}

- [JSON functions](../yql/reference/builtins/json.md) — `JSON_EXISTS`, `JSON_VALUE`, `JSON_QUERY`, JsonPath syntax.
- [Secondary indexes](secondary-indexes.md) — general information about global indexes and `VIEW`.
- [Full-text indexes](fulltext-indexes.md) — a related mechanism built on top of an inverted index of words and phrases.
- [INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/json_index.md) and [VIEW (JSON index)](../yql/reference/syntax/select/json_index.md) — syntax reference.
