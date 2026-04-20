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

* Tables with fulltext indexes only support `Uint64` primary keys.
* `BulkUpsert` isn't supported for tables with fulltext indexes.
* Fulltext index access must be specified explicitly using `VIEW IndexName`.
* Only one text column can be indexed (per fulltext index). Use `COVER` for additional columns.
* `FulltextMatch` / `FulltextScore` can't be used with `OR` or `NOT`. Combining them with other predicates via `AND` is supported.
* A single read through `VIEW` supports only one fulltext predicate: multiple `FulltextScore` calls are not supported, and mixing `FulltextMatch` and `FulltextScore` in the same `WHERE` is not supported.
* For relevance access, you must include `FulltextScore(...) > 0` in `WHERE` (otherwise the query fails).
