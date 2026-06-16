# Hybrid search

## Hybrid search concept

**Hybrid search** combines [fulltext search](fulltext_search.md) and [vector search](vector_search.md) into a single ranked result. Instead of choosing one signal, a hybrid query scores each document by both how well its text matches the query (lexical relevance) and how close its embedding is to the query vector (semantic similarity), and then **fuses** the two rankings into one.

The two signals are complementary:

* **Fulltext search** ([BM25](https://en.wikipedia.org/wiki/Okapi_BM25)) is precise for exact terms, names, codes, and rare keywords, but misses documents that use different wording.
* **Vector search** (nearest neighbor over embeddings) captures semantic similarity and paraphrases, but can rank a topically related document above one that contains the exact term the user typed.

Fusing them yields results that are both lexically and semantically relevant. This is a common building block for search over knowledge bases and for the retrieval stage of Retrieval-Augmented Generation (RAG).

## Hybrid search in {{ ydb-short-name }} {#hybrid-search-ydb}

In {{ ydb-short-name }}, a hybrid query runs over a table that has **both**:

* a [fulltext_relevance](../../dev/fulltext-indexes.md#relevance) index over the text column, and
* a [vector_kmeans_tree](../../dev/vector-indexes.md) index over the embedding column.

The query expresses the fusion in the `ORDER BY` clause with the `HybridRank` function, which takes one scoring expression per branch — a [FullTextScore](../../yql/reference/builtins/fulltext.md#fulltext-score) for the text branch and a [Knn](../../yql/reference/udf/list/knn.md) distance or similarity for the vector branch:

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector))
LIMIT 10;
```

Each branch runs against its own index and returns its own list of best-matching documents. {{ ydb-short-name }} then merges these lists into a single ranking — in one of two ways:

* **Reciprocal Rank Fusion (RRF)** — the default. Each branch contributes a term based on a document's *rank* within that branch, so the absolute magnitudes of the scores (which are not comparable across branches) do not matter.
* **Linear** — a weighted sum of the (optionally min-max normalized) per-branch scores.

Per-branch weights, the RRF constant, and the candidate-pool sizes are all configurable. For the full query syntax and parameters, see [{#T}](../../yql/reference/syntax/select/hybrid_search.md).

Learn more:

* [Using hybrid search](../../dev/hybrid-search.md)
* [Hybrid search query syntax (HybridRank)](../../yql/reference/syntax/select/hybrid_search.md)
* [Fulltext search](fulltext_search.md)
* [Vector search](vector_search.md)
