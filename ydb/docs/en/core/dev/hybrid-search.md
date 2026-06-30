# Hybrid search

Hybrid search combines [fulltext search](fulltext-indexes.md) and [vector search](vector-indexes.md) into a single ranked result: each document is scored by both its text relevance and its embedding similarity, and the two rankings are fused into one. This brings together the precision of lexical matching and the recall of semantic similarity, and is a common building block for the retrieval stage of Retrieval-Augmented Generation (RAG).

For the general idea of hybrid search, see [Hybrid search concept](../concepts/query_execution/hybrid_search.md).

Hybrid search is not a separate index type. It reuses two existing indexes on the same table:

* a [fulltext_relevance](fulltext-indexes.md#relevance) index over the text column — provides the [BM25](https://en.wikipedia.org/wiki/Okapi_BM25) relevance signal;
* a [vector_kmeans_tree](vector-indexes.md) index over the embedding column — provides the nearest-neighbor (semantic) signal.

## Preparing the indexes {#prepare}

Create a table with a text column and an embedding column, then add both indexes.

```yql
CREATE TABLE documents (
    id Uint64,
    text Utf8,
    embedding String,
    PRIMARY KEY (id)
);
```

Add a `fulltext_relevance` index over the text column (relevance scoring requires this type, not `fulltext_plain`):

```yql
ALTER TABLE documents
  ADD INDEX ft_idx
  GLOBAL USING fulltext_relevance
  ON (text)
  WITH (tokenizer=standard, use_filter_lowercase=true);
```

Add a `vector_kmeans_tree` index over the embedding column:

```yql
ALTER TABLE documents
  ADD INDEX vec_idx
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine);
```

For details on each index type, see [{#T}](fulltext-indexes.md) and [{#T}](vector-indexes.md).

## Running a hybrid query {#query}

A hybrid query is a regular `SELECT` over the base table (without `VIEW`) whose `ORDER BY` key is a single `HybridRank` call. `HybridRank` takes one scoring expression per branch: a [FullTextScore](../yql/reference/builtins/fulltext.md#fulltext-score) for the text branch and a [Knn](../yql/reference/udf/list/knn.md) distance or similarity for the vector branch.

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, text
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector))
LIMIT 10;
```

Both inputs come from the same user query: `$queryText` is the search text (matched lexically), and `$queryVector` is its embedding computed by the application (matched semantically). {{ ydb-short-name }} cannot compute embeddings itself, so the vector is passed in — here it is built from a literal via [Knn::ToBinaryStringFloat](../yql/reference/udf/list/knn.md#functions-convert), but in an application it is the output of an embedding model.

{{ ydb-short-name }} resolves each branch to its index automatically by the scored column (`text` → the fulltext relevance index, `embedding` → the vector index), retrieves a candidate pool from each, and fuses the two rankings. The result is the top `LIMIT` documents by the fused score.

Note that, unlike fulltext and vector search, a hybrid query does **not** use `VIEW IndexName`: the indexes are selected from the `HybridRank` arguments, and the query reads through the base table.

`PRAGMA ydb.KMeansTreeSearchTopSize` controls the recall of the vector branch — see [KMeansTreeSearchTopSize](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize). As with plain vector search, it should be set explicitly.

## Tuning the fusion {#tuning}

The fusion method and its parameters are passed as named arguments to `HybridRank`. The most common ones:

* `Mode` — `"rrf"` (default, Reciprocal Rank Fusion) or `"linear"` (weighted sum of normalized scores);
* `Weights` — a per-branch weight tuple, one value per scoring argument, to bias the ranking toward one signal;
* `K` — the RRF constant (default `60.0`);
* `Indexes` / `Limits` — explicit per-branch index names and candidate-pool sizes.

For the built-in modes, the per-branch contribution is fixed by the formula. For full control over the fusion, pass a custom lambda instead of `Mode`:

* `RankLambda` — a lambda that receives each document's per-branch ranks (1-based position within each branch) and returns the fused score;
* `ScoreLambda` — a lambda that receives each document's per-branch raw scores (the fulltext relevance or vector distance/similarity) and returns the fused score.

A custom lambda replaces the built-in fusion, so it cannot be combined with `Mode`, `Weights`, `K`, or `Normalize` — fold any weights and constants into the lambda body. For a detailed description and examples, see [Hybrid search query syntax (HybridRank)](../yql/reference/syntax/select/hybrid_search.md#custom-fusion).

For example, to weight the vector branch twice as much as the text branch under RRF:

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, text
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    (1, 2) AS Weights)
LIMIT 10;
```

For the full list of parameters and their semantics, see [{#T}](../yql/reference/syntax/select/hybrid_search.md).

## Limitations {#limitations}

* The table must have both a ready `fulltext_relevance` index and a non-prefixed `vector_kmeans_tree` index over the respective columns; otherwise the query fails with a clear message.
* [Prefixed vector indexes](vector-indexes.md) are not supported yet.
* If more than one fulltext (or vector) index matches a branch's column, the branch is ambiguous and must be disambiguated with an explicit `AS Indexes` override.
* `LIMIT` must be a literal, because it sizes the per-branch candidate pools. To use a parameterized `LIMIT`, pass explicit `AS Limits`.
* `HybridRank(...)` must be the entire `ORDER BY` key — it cannot be negated, wrapped, or combined with other sort keys.
* A custom fusion lambda (`RankLambda` or `ScoreLambda`) replaces the built-in fusion and cannot be combined with `Mode`, `Weights`, `K`, or `Normalize`. At most one of `RankLambda` or `ScoreLambda` may be specified.
* A custom fusion lambda (`RankLambda` or `ScoreLambda`) replaces the built-in fusion and cannot be combined with `Mode`, `Weights`, `K`, or `Normalize`. At most one of `RankLambda` or `ScoreLambda` may be specified.
