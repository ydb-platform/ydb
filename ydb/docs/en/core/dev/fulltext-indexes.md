# Fulltext Indexes

Fulltext indexes are specialized data structures that enable efficient text search within table columns. Unlike [secondary indexes](../concepts/glossary.md#secondary-index), which optimize searching by equality or range, fulltext indexes allow searching by words, phrases, and (with n-grams) by substrings.

For the general idea of fulltext search, see [Fulltext search](../concepts/fulltext_search.md).

## Characteristics of Fulltext Indexes {#characteristics}

Fulltext indexes in {{ ydb-short-name }} are built by tokenizing text and creating an inverted index. This enables:

* fast filtering with `FulltextMatch()`
* relevance ranking ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) with `FulltextScore()` when using `fulltext_relevance`
* case normalization, stemming, and n-gram matching via index filters

The current implementation supports two indexes:

* `fulltext_plain` — basic fulltext index
* `fulltext_relevance` — fulltext index with [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) statistics for relevance scoring

Additionally, a fulltext index can be **covering** (via `COVER`), meaning it includes a copy of extra columns from the base table.


## Types of Fulltext Indexes {#types}

### Basic fulltext index (`fulltext_plain`) {#basic}

A global fulltext index on the `body` column for filtering with `FulltextMatch()`:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_plain
  ON (body)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Example query:

```yql
SELECT id, title
FROM articles VIEW ft_index
WHERE FulltextMatch(body, "search terms")
ORDER BY id;
```

### Fulltext index for ranking (`fulltext_relevance`) {#relevance}

For relevance ranking, use `fulltext_relevance` and `FulltextScore()`:

```yql
ALTER TABLE articles
  ADD INDEX ft_index
  GLOBAL USING fulltext_relevance
  ON (body) COVER (title)
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

If you need substring search, create the index with n-grams (`use_filter_ngram` / `use_filter_edge_ngram`). This enables:

* `FulltextMatch(..., "Wildcard" AS Mode)` (patterns with `%` / `_`)
* `LIKE` / `ILIKE` predicates over the indexed text column (they use the index)

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
ORDER BY id;
```

Example query with `LIKE`:

```yql
SELECT id, title
FROM articles VIEW ngram_index
WHERE body LIKE "%learn%ing%"
ORDER BY id;
```

## Full syntax for fulltext indexes {#syntax}

Creating a fulltext index:

* during table creation: [CREATE TABLE](../yql/reference/syntax/create_table/fulltext_index.md)
* adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md)

Full syntax for querying a fulltext index:

* [VIEW (Fulltext index)](../yql/reference/syntax/select/fulltext_index.md)

Functions and expressions for fulltext search:

* [Fulltext built-in functions](../yql/reference/builtins/fulltext.md)
* [LIKE / ILIKE with a fulltext index](../yql/reference/syntax/expressions.md#like-ilike-with-fulltext-index)

## Index parameters {#parameters}

Fulltext index parameters are specified in the `WITH (...)` clause.
For the full list, see:

* [FULLTEXT INDEX (CREATE TABLE)](../yql/reference/syntax/create_table/fulltext_index.md)

## Using Fulltext Indexes {#select}

Queries to fulltext indexes are executed using the `VIEW` syntax in YQL.

For full details and additional parameters, see:

* [VIEW (Fulltext index)](../yql/reference/syntax/select/fulltext_index.md)
* [Fulltext built-in functions](../yql/reference/builtins/fulltext.md)

{% note info %}

The optimizer doesn't select a fulltext index automatically, so you must specify it explicitly using `VIEW IndexName`.

If the `VIEW` expression is not used, `FulltextMatch` / `FulltextScore` queries will fail.

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

* `BulkUpsert` isn't supported for tables with fulltext indexes.
* Fulltext index access must be specified explicitly using `VIEW IndexName`.
* Only one text column can be indexed (per fulltext index). Use `COVER` for additional columns.
* `FulltextMatch` / `FulltextScore` can't be used with `OR` or `NOT`. Combining them with other predicates via `AND` is supported.
* A single read through `VIEW` supports only one fulltext predicate: multiple `FulltextScore` calls are not supported, and mixing `FulltextMatch` and `FulltextScore` in the same `WHERE` is not supported.
* For relevance access, you must include `FulltextScore(...) > 0` in `WHERE` (otherwise the query fails).
