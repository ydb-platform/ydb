# Fulltext Indexes

Fulltext indexes are a specialized type of [secondary index](../concepts/glossary.md#secondary-index) that enable efficient text search within table columns. While traditional secondary indexes optimize searching by equality or range, fulltext indexes allow searching by words, phrases, and (with n-grams) by substrings.

For the general idea of fulltext search, see [Fulltext search](../concepts/query_execution/fulltext_search.md).

## Characteristics of Fulltext Indexes {#characteristics}

Fulltext indexes in {{ ydb-short-name }} are built by tokenizing text and creating an inverted index. This enables:

* fast filtering with [FulltextMatch](../yql/reference/builtins/fulltext.md#fulltext-match)
* relevance ranking ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) with [FulltextScore](../yql/reference/builtins/fulltext.md#fulltext-score) when using [fulltext_relevance](#relevance)
* case normalization, stemming, and n-gram matching via index filters

The current implementation supports two indexes:

* [fulltext_plain](#basic) — basic fulltext index
* [fulltext_relevance](#relevance) — fulltext index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance scoring

Additionally, a fulltext index can be **covering** (via `COVER`), meaning it includes a copy of extra columns from the base table.


## Types of Fulltext Indexes {#types}

{{ ydb-short-name }} supports two types of fulltext indexes, differing in the statistics they store:

* [fulltext_plain](#basic) — stores only the inverted index. Supports filtering via [FulltextMatch](../yql/reference/builtins/fulltext.md#fulltext-match), but does not support relevance ranking.
* [fulltext_relevance](#relevance) — additionally stores term frequency statistics (TF-IDF / [BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) required by [FulltextScore](../yql/reference/builtins/fulltext.md#fulltext-score).

Indexes of either type can also be [filtered](#filtered) to scope search to a specific logical partition.

### Basic fulltext index (`fulltext_plain`) {#basic}

Use `fulltext_plain` when you only need to check whether terms are present in the text, without relevance ranking. This index is more compact than `fulltext_relevance` and is suitable for most filtering tasks.

Example: create a global fulltext index on the `body` column:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_plain
  ON (body)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Here `tokenizer=standard` splits text into words on whitespace and punctuation, and `use_filter_lowercase=true` normalizes all tokens to lowercase, making the search case-insensitive.

Example query:

```yql
SELECT id, title
FROM articles VIEW ft_index
WHERE FulltextMatch(body, "search terms")
LIMIT 20;
```

### Fulltext index for ranking (`fulltext_relevance`) {#relevance}

`fulltext_relevance` stores the inverted index along with term frequency statistics ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)), which allows [FulltextScore](../yql/reference/builtins/fulltext.md#fulltext-score) to compute a relevance score for each document. Use this type when you need not just to find documents containing certain words, but also to rank them by how well they match the query.

Example index:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_relevance
  ON (body)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Example ranking query:

```yql
SELECT id, title, FulltextScore(body, "search terms") AS relevance
FROM articles VIEW ft_index
WHERE FulltextScore(body, "search terms") > 0
ORDER BY relevance DESC
LIMIT 10;
```

### Substring search (n-grams) {#substr}

If you need substring search, create the index with n-grams. Two types of n-grams are available:

* **Regular n-grams** (`use_filter_ngram`) — split words into all possible substrings of specified length, allowing matches anywhere within a word. For example, the word "search" will be split into "sea", "ear", "arc", "rch", etc.
* **Edge n-grams** (`use_filter_edge_ngram`) — create substrings only from the beginning of words, which is ideal for autocomplete functionality. For example, the word "search" will be split into "se", "sea", "sear", "searc", "search".

When using n-grams, the following becomes available:

* [FulltextMatch(..., "Wildcard" AS Mode)](../yql/reference/builtins/fulltext.md#fulltext-match) — patterns with `%` and `_` (similar to `LIKE`)
* `LIKE` / `ILIKE` predicates over the indexed text column — {{ ydb-short-name }} automatically uses the n-gram index when accessed via `VIEW IndexName`

Example index with n-grams:

```yql
ALTER TABLE articles
  ADD INDEX ngram_index
  GLOBAL USING fulltext_plain
  ON (body)
  WITH (
    tokenizer=standard,
    use_filter_lowercase=true,
    use_filter_ngram=true,
    filter_ngram_min_length=3,
    filter_ngram_max_length=5
  );
```

Example query with `FulltextMatch`:

```yql
SELECT id, title
FROM articles VIEW ngram_index
WHERE FulltextMatch(body, "%learn%", "Wildcard" AS Mode)
LIMIT 20;
```

Example query with `LIKE`:

```yql
SELECT id, title
FROM articles VIEW ngram_index
WHERE body LIKE "%learn%ing%"
LIMIT 20;
```

A `LIKE` / `ILIKE` query uses the same logic as `FulltextMatch(body, ..., "Wildcard" AS Mode)` and accesses the same n-gram index.

### Filtered fulltext index {#filtered}

A filtered fulltext index enables fulltext search within each logical partition defined by filter columns. To create such an index, specify one or more filter columns before the text column in the `ON` clause. The last column must be the text column; the others can be of any comparable type:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_plain
  ON (user_id, body)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Search queries using a filtered index must include an equality predicate on every filter column:

```yql
SELECT id, title
FROM articles VIEW ft_index
WHERE user_id = 42 AND FulltextMatch(body, "search terms")
LIMIT 20;
```

Multiple filter columns are supported. The equality predicates may appear in any order in `WHERE`; {{ ydb-short-name }} reorders them internally to match the index column order.

## Primary key types {#primary-key}

Inside the inverted index, every indexed document is identified by a numeric document id (`doc_id`). How {{ ydb-short-name }} derives the `doc_id` depends on the base table's [primary key](../concepts/glossary.md#primary-key):

* **Single integer primary key** (`Uint64`, `Int64`, `Uint32`, or `Int32`) — the primary key value is used directly as the `doc_id`. This is the most compact form, and no additional structures are created.
* **Any other primary key** (for example, `Utf8`, `String`, other non-integer types, or a composite key of several columns) — {{ ydb-short-name }} maintains a system column `__ydb_row_id` of type `Uint64` and uses it as the `doc_id`.

When you create a fulltext index on a table whose primary key is not a single integer column, {{ ydb-short-name }} automatically:

* adds the `__ydb_row_id` column to the table — existing rows are backfilled while the index is being built;
* generates a `__ydb_row_id` value for every row where the column is omitted from `INSERT` / `UPSERT`;
* creates a unique [secondary index](../concepts/glossary.md#secondary-index) named `__ydb_unique_row_id` over `__ydb_row_id`. At query time this index maps a matched `__ydb_row_id` back to the table's primary key before the row is read from the main table.

If the table already has more than one fulltext index, they all **reuse** the same `__ydb_row_id` column and `__ydb_unique_row_id` index — these structures are created only once per table.

{% note warning %}

The `__ydb_row_id` column and the `__ydb_unique_row_id` index are managed by {{ ydb-short-name }}:

* Let {{ ydb-short-name }} populate `__ydb_row_id` — omit it from `INSERT` / `UPSERT` and it is filled in automatically. User modification of the `__ydb_row_id` column is forbidden — any attempts to set or change the value of this column will be rejected.
* The `__ydb_unique_row_id` index cannot be dropped while any fulltext index depends on it. Drop the dependent fulltext index(es) first.

{% endnote %}

## Full syntax for fulltext indexes {#syntax}

Creating a fulltext index:

* during table creation: [CREATE TABLE](../yql/reference/syntax/create_table/fulltext_index.md)
* adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md)

Full syntax for querying a fulltext index:

* [VIEW (Fulltext index)](../yql/reference/syntax/select/fulltext_index.md)

Functions and expressions for fulltext search:

* [Fulltext built-in functions](../yql/reference/builtins/fulltext.md)
* [LIKE / ILIKE with a fulltext index](../yql/reference/syntax/expressions.md#like-ilike-with-fulltext-index)

{% note info %}

The optimizer doesn't select a fulltext index automatically, so you must specify it explicitly using `VIEW IndexName`.

If the `VIEW` expression is not used, `FulltextMatch` / `FulltextScore` queries will fail.

This limitation may be removed in future versions of {{ ydb-short-name }}.

{% endnote %}

## Updating Fulltext Indexes {#update}

Fulltext indexes are maintained automatically on data modifications. Tables with fulltext indexes support:

* `INSERT`
* `UPSERT`
* `REPLACE`
* `UPDATE`
* `DELETE`

## Dropping Fulltext Indexes {#drop}

```yql
ALTER TABLE articles DROP INDEX ft_index;
```

## Limitations {#limitations}

* Tables with a non-integer or composite primary key get an auto-managed `__ydb_row_id` column and `__ydb_unique_row_id` unique index (see [Primary key types](#primary-key)).
* `BulkUpsert` isn't supported for tables with fulltext indexes.
* Fulltext index access must be specified explicitly using `VIEW IndexName`.
* Only one text column can be indexed (per fulltext index). Use `COVER` for additional columns.
* `FulltextMatch` / `FulltextScore` can't be used with `OR` or `NOT`. Combining them with other predicates via `AND` is supported.
* A single read through `VIEW` supports only one fulltext predicate: multiple `FulltextScore` calls are not supported, and mixing `FulltextMatch` and `FulltextScore` in the same `WHERE` is not supported.
* For relevance access, you must include `FulltextScore(...) > 0` in `WHERE` (otherwise the query fails).
* [Filtered fulltext indexes](#filtered): every filter column needs an equality predicate in `WHERE`.
* [Filtered fulltext indexes](#filtered): filter columns must not be primary key columns.
