# Hybrid search (HybridRank)

[Hybrid search](../../../../dev/hybrid-search.md) fuses [fulltext](../../../../dev/fulltext-indexes.md) and [vector](../../../../dev/vector-indexes.md) ranking into a single result. A hybrid query is a `SELECT` over the base table whose `ORDER BY` key is a single `HybridRank` call:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector))
LIMIT 10;
```

{% note info %}

A hybrid query reads through the base table and does **not** use `VIEW IndexName`: each branch's index is resolved from the corresponding `HybridRank` argument.

The query requires both a [fulltext_relevance](../../../../dev/fulltext-indexes.md#relevance) index over the column passed to `FullTextScore` and a (non-prefixed) [vector_kmeans_tree](../../../../dev/vector-indexes.md) index over the column passed to `Knn`. The recall of the vector branch is controlled by [KMeansTreeSearchTopSize](vector_index.md#KMeansTreeSearchTopSize).

{% endnote %}

## HybridRank {#hybrid-rank}

The general form of the function:

```yql
HybridRank(
    score1, score2 [, ... scoreN]        -- 2+ scoring expressions: FullTextScore(...) or Knn::<Distance|Similarity>(...)
    [, "rrf" | "linear"  AS Mode]        -- fusion method (default "rrf")
    [, (w1, w2, ...)     AS Weights]     -- per-branch weights (default 1.0 each)
    [, k                 AS K]           -- RRF constant (default 60.0)
    [, true | false      AS Normalize]   -- "linear" mode only: min-max normalize (default true)
    [, (idx1, idx2, ...) AS Indexes]     -- per-branch index names (default: auto-detect)
    [, (lim1, lim2, ...) AS Limits]      -- per-branch candidate-pool sizes (default: LIMIT * 10)
)
```

`HybridRank` may only appear as the entire sort key of an `ORDER BY` clause. It takes **two or more scoring expressions** (positional), followed by optional **named arguments**.

Each scoring expression is one *branch* of the fusion, classified by its form:

* a [FullTextScore(text, query)](../../builtins/fulltext.md#fulltext-score) expression — a **fulltext** branch, resolved against a `fulltext_relevance` index over the `text` column;
* a [Knn](../../udf/list/knn.md) distance or similarity over a column (for example `Knn::CosineDistance(embedding, $queryVector)` or `Knn::CosineSimilarity(embedding, $queryVector)`) — a **vector** branch, resolved against a `vector_kmeans_tree` index over that column.

The argument order is not significant, and there may be more than two branches (for example a fulltext branch plus two vector branches). Each branch retrieves its own candidate pool and contributes one term to every document's fused score.

### Fusion modes {#modes}

Two fusion methods are available via the `Mode` argument:

* **`rrf`** (default) — [Reciprocal Rank Fusion](https://learn.microsoft.com/azure/search/hybrid-search-ranking#how-rrf-ranking-works). Each branch contributes `weight / (K + rank)`, where `rank` is the document's position within that branch. Because RRF uses ranks rather than raw scores, the (non-comparable) magnitudes of the branch scores do not matter.
* **`linear`** — a weighted sum of the per-branch scores: `weight * score` summed over branches. By default the per-branch scores are min-max normalized to `[0, 1]` before summing; set `Normalize` to `false` to fuse the raw scores.

In both modes a document that is absent from a branch's candidate pool simply does not receive that branch's contribution.

### Named arguments {#named-args}

All optional parameters are passed as named arguments. The `Weights`, `Limits`, and `Indexes` arguments are tuples positionally parallel to the scoring expressions — they must have exactly one entry per branch.

| Argument | Type | Description |
|----------|------|-------------|
| `Mode` | String | Fusion method: `"rrf"` (default) or `"linear"`. Case-insensitive (`"RRF"` / `"Linear"` are accepted). |
| `Weights` | Tuple of numbers | Per-branch weight, one per scoring argument. Default `1.0` for every branch. A weight of `0` disables a branch. |
| `K` | Double | The RRF constant (`rrf` mode only). Default `60.0`. Larger values flatten the influence of top ranks. |
| `Normalize` | Bool | `linear` mode only. When `true` (default), per-branch scores are min-max normalized before summing; when `false`, raw scores are fused. |
| `Indexes` | Tuple of strings | Explicit index name per branch, overriding auto-detection. Required when a branch's column is covered by more than one matching index. |
| `Limits` | Tuple of positive integers | Explicit candidate-pool size per branch. By default each branch retrieves `LIMIT * HybridSearchFactor` (factor default `10`) candidates. |

### Examples {#examples}

Weighted RRF — bias the ranking toward the vector branch:

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    (1, 2) AS Weights)
LIMIT 10;
```

Linear fusion over normalized scores:

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    "linear" AS Mode)
LIMIT 10;
```

Explicit index names and candidate-pool sizes. Because the per-branch pool sizes are given by `Limits`, the query's own `LIMIT` no longer needs to be a literal and may be a parameter:

```yql
DECLARE $limit AS Uint64;

$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ("ft_idx", "vec_idx") AS Indexes,
    (100, 200) AS Limits)
LIMIT $limit;
```

## Limitations {#limitations}

* `HybridRank(...)` must be the entire `ORDER BY` key; it cannot be negated, nested in a larger expression, or combined with other sort keys.
* At least two scoring arguments are required — a single branch is not a hybrid query.
* `LIMIT` must be a literal (it sizes the per-branch candidate pools). Use an explicit `AS Limits` to allow a parameterized `LIMIT`.
* [Prefixed vector indexes](../../../../dev/vector-indexes.md) are not supported yet.
