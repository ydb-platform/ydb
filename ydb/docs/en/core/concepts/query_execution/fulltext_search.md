# Fulltext search

## Concept of fulltext search

**Fulltext search** is a way to find documents by text content in a `String` or `Utf8` column by words, phrases, and (with special indexing) by substrings. Unlike simple string predicates, fulltext search typically involves **tokenization** and **normalization** (for example, lowercasing), so queries operate on "terms" rather than raw byte substrings. Typical use cases include:

* search in product catalogs and knowledge bases
* log/message search
* searching document metadata and descriptions

The core idea of a fulltext index is an **[inverted index](https://en.wikipedia.org/wiki/Inverted_index)**: for each term, the index stores the list of documents (primary keys) that contain that term. This allows queries like "documents containing terms A and B" to avoid full table scans.

In {{ ydb-short-name }}, fulltext search can be performed in two main ways:

* **without an index** — by scanning the table and applying string predicates (for example, `LIKE`). This approach is simple but does not scale well as data grows.
* **with a fulltext index** — by creating a [fulltext index](../../dev/fulltext-indexes.md) and executing queries through `VIEW IndexName`. This approach is designed for scalable search.

## Fulltext search with a fulltext index {#fulltext-search-index}

Fulltext indexes build an inverted index over a text column and allow:

* fulltext matching via [FulltextMatch](../../yql/reference/builtins/fulltext.md#fulltext-match) (query modes `Keywords` / `Query` / `Wildcard`, default operator `And` / `Or`)
* relevance ranking ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) via [FulltextScore](../../yql/reference/builtins/fulltext.md#fulltext-score) when using [fulltext_relevance](../../dev/fulltext-indexes.md#relevance)

Search behavior (what counts as a term, case/word-form handling, wildcard support) is configured at index creation time via tokenizers and filters (for example, lowercase, Snowball stemming, n-grams). For details, see the index creation syntax [{#T}](../../yql/reference/syntax/create_table/fulltext_index.md).

Learn more:

* [Fulltext indexes](../../dev/fulltext-indexes.md)
* [Fulltext built-in functions](../../yql/reference/builtins/fulltext.md)
* [VIEW (Fulltext index)](../../yql/reference/syntax/select/fulltext_index.md)
* [Bloom skip indexes and filtering](bloom_skip_indexes.md) — selective filtering by values and substrings without an inverted fulltext index
