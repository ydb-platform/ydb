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

Each `FullTextScore` branch requires a ready [fulltext_relevance](../../../../dev/fulltext-indexes.md#relevance) index. Each `Knn` branch requires a ready [vector_kmeans_tree](../../../../dev/vector-indexes.md) index with a compatible metric. Both index types can have prefixes; see [index selection and filters](#indexes-and-filters). The recall of the vector branch is controlled by [KMeansTreeSearchTopSize](vector_index.md#KMeansTreeSearchTopSize).

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
    [, ($x) -> {...}     AS RankLambda]  -- custom fusion over per-branch ranks (replaces built-in Mode)
    [, ($x) -> {...}     AS ScoreLambda] -- custom fusion over per-branch raw scores (replaces built-in Mode)
)
```

`HybridRank` may only appear as the entire sort key of an `ORDER BY` clause. It takes **two or more scoring expressions** (positional), followed by optional **named arguments**.

Each scoring expression is one *branch* of the fusion, classified by its form:

* a [FullTextScore(text, query)](../../builtins/fulltext.md#fulltext-score) expression — a **fulltext** branch, resolved against a `fulltext_relevance` index over the `text` column;
* a [Knn](../../udf/list/knn.md) distance or similarity over a column (for example `Knn::CosineDistance(embedding, $queryVector)` or `Knn::CosineSimilarity(embedding, $queryVector)`) — a **vector** branch, resolved against a `vector_kmeans_tree` index over that column.

The argument order is not significant, and there may be more than two branches (for example a fulltext branch plus two vector branches). Each branch retrieves its own candidate pool. Fusion considers the union of these pools and returns each document once, including documents found by only one branch. A `NULL` scored column excludes a document from that branch, but another branch can still retrieve it.

Use a bare `FullTextScore` call as a scoring argument. Expressions such as `2 * FullTextScore(...)` are rejected; use `Weights` or `ScoreLambda` to change the contribution. The call supports the named options `DefaultOperator`, `MinimumShouldMatch`, `K1`, and `B` described in [FullTextScore](../../builtins/fulltext.md#fulltext-score). `K1` and `B` accept `Float` or `Double` literals and parameters; `DefaultOperator` and `MinimumShouldMatch` accept string literals and parameters.

Supported vector functions are `Knn::CosineDistance`, `Knn::CosineSimilarity`, `Knn::ManhattanDistance`, `Knn::EuclideanDistance`, and `Knn::InnerProductSimilarity`.

### Fusion modes {#modes}

Two fusion methods are available via the `Mode` argument:

* **`rrf`** (default) — [Reciprocal Rank Fusion](https://learn.microsoft.com/azure/search/hybrid-search-ranking#how-rrf-ranking-works). Each branch contributes `weight / (K + rank)`, where `rank` is the document's position within that branch. Because RRF uses ranks rather than raw scores, the (non-comparable) magnitudes of the branch scores do not matter.
* **`linear`** computes a weighted sum of per-branch scores, with larger values always meaning a better match. With `Normalize = true` (default), normalization uses the minimum and maximum within each branch's candidate pool. Relevance and similarity use `(score - min) / (max - min)`; distances use `(max - score) / (max - min)`. A branch whose scores are all equal contributes `0`. With `Normalize = false`, relevance and similarity retain their raw values, while distances are negated before weighting.

In both modes a document that is absent from a branch's candidate pool simply does not receive that branch's contribution.

The final result is ordered by decreasing fused score. Documents with equal fused scores have no guaranteed relative order. A weight of `0` removes that branch's score contribution, but its candidates can still appear in the result.

### Custom fusion lambda {#custom-fusion}

Instead of the built-in `Mode` (`rrf`/`linear`), you can supply a custom fusion lambda that computes the fused score from the per-branch values yourself. Two mutually exclusive lambda kinds are available:

* `RankLambda` — the lambda receives the document's per-branch ranks as a `Dict<Int64, Int64>` (branch index → 1-based rank within that branch). A branch the document is absent from has no entry, so `$ranks[i]` is `NULL`.
* `ScoreLambda` — the lambda receives the document's per-branch raw scores as a `Dict<Int64, Double>` (branch index → the branch's score value: the fulltext relevance, or the vector distance/similarity). A branch the document is absent from has no entry, so `$scores[i]` is `NULL`.

The lambda takes a single argument (the dict) and must return a numeric value (`Double` is recommended); larger values rank higher. The lambda replaces the built-in fusion, so it cannot be combined with `Mode`, `Weights`, `K`, or `Normalize` — fold any weights and constants into the lambda body.

```yql
SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($ranks) -> {
        RETURN 1.0 / (60 + COALESCE($ranks[0], 100000))
             + 1.0 / (60 + COALESCE($ranks[1], 100000));
    } AS RankLambda)
LIMIT 10;
```

### Named arguments {#named-args}

All optional parameters are passed as named arguments. Except for fusion lambdas, their values must be literals. The `Weights`, `Limits`, and `Indexes` arguments are tuples positionally parallel to the scoring expressions — they must have exactly one entry per branch. Weights and the RRF constant must be finite; RRF also requires a nonzero `K + rank`.

| Argument | Type | Description |
|----------|------|-------------|
| `Mode` | String | Fusion method: `"rrf"` (default) or `"linear"`. Case-insensitive (`"RRF"` / `"Linear"` are accepted). |
| `Weights` | Tuple of numbers | Per-branch weight, one per scoring argument. Default `1.0` for every branch. A weight of `0` removes the score contribution while preserving the branch's candidates. |
| `K` | Double | The RRF constant (`rrf` mode only). Default `60.0`. Larger values flatten the influence of top ranks. |
| `Normalize` | Bool | `linear` mode only. When `true` (default), per-branch scores are min-max normalized and distances are inverted. When `false`, raw relevance and similarity scores are used and raw distances are negated. |
| `Indexes` | Tuple of strings | Explicit index name per branch, overriding [automatic selection](#indexes-and-filters). Required when automatic selection is ambiguous. |
| `Limits` | Tuple of positive integers | Explicit candidate-pool size per branch. By default each branch retrieves up to `LIMIT * HybridSearchFactor` (factor default `10`) candidates. |
| `RankLambda` | Lambda | Custom fusion over per-branch ranks. The lambda receives a `Dict<Int64, Int64>` (branch index → 1-based rank; absent branches have no entry) and returns a numeric score. Replaces the built-in `Mode`; cannot be combined with `Mode`, `Weights`, `K`, or `Normalize`. |
| `ScoreLambda` | Lambda | Custom fusion over per-branch raw scores. The lambda receives a `Dict<Int64, Double>` (branch index → raw branch score; absent branches have no entry) and returns a numeric score. Replaces the built-in `Mode`; cannot be combined with `Mode`, `Weights`, `K`, or `Normalize`. |

### Index selection and filters {#indexes-and-filters}

Automatic selection considers ready indexes whose final indexed column is the column used by the scoring expression. A prefixed index is eligible only when `WHERE` fixes every prefix column to a value using equality predicates joined by `AND`.

* For a fulltext branch, exactly one eligible relevance index must remain. Both legacy and compact relevance indexes are supported.
* For a vector branch, the metric must also match the `Knn` function. Among compatible indexes, the index with the longest fully bound prefix is preferred. Indexes with an unbound or partially bound prefix are skipped. If several indexes have the same longest prefix length, selection is ambiguous.

Use `AS Indexes` to resolve ambiguity. Explicit index names still undergo column, metric, readiness, and prefix checks.

For example, with indexes on `(category, text)` and `(category, embedding)`, select one category before ranking:

```yql
DECLARE $category AS Utf8;
DECLARE $queryText AS Utf8;
DECLARE $queryVector AS String;

SELECT id, title
FROM documents
WHERE category = $category
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ("ft_by_category", "vec_by_category") AS Indexes,
    (100, 100) AS Limits)
LIMIT 10;
```

Prefix values can be literals, parameters, or scalar fields of non-optional struct parameters. Nullable table columns can be prefixes when compared with non-optional values. Every prefix column must be bound, including columns that also belong to the primary key. A leading subset of a prefix, equality under `OR`, or a field taken from an optional struct parameter does not satisfy this requirement in the current implementation.

Prefix predicates restrict the corresponding index branch. Other `WHERE` predicates are applied after candidate fusion and the lookup of base-table rows, before the final `LIMIT`. They can reduce the result below the requested size; increase the candidate pools if necessary. A fulltext-scored column itself cannot be fixed by an equality predicate in `WHERE`, because all surviving documents would have the same text value for that branch.

### Candidate pools and pagination {#candidate-pools}

Set `PRAGMA ydb.HybridSearchFactor = "20";` to retrieve up to twenty times the final limit from each branch, or use `AS Limits` to set each pool explicitly. These settings control how many candidates are fused; [KMeansTreeSearchTopSize](vector_index.md#KMeansTreeSearchTopSize) separately controls exploration of the vector index.

Changing the final `LIMIT` also changes the default candidate pools and can change the ranking. For pagination, use fixed explicit pools large enough for the requested page. `OFFSET` is applied to the fused ranking. Vector search is approximate, and tied scores have no guaranteed order, so a stable total order across separate queries is not guaranteed.

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

Three-branch RRF — combine text relevance with two vector branches over different embedding columns:

```yql
$queryText = "machine learning";
$titleVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);
$bodyVector = Knn::ToBinaryStringFloat([0.7, 0.8, 0.9, 1.0]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(title_embedding, $titleVector),
    Knn::CosineDistance(body_embedding, $bodyVector),
    (1, 2, 3) AS Weights)
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

Custom `RankLambda` — spell out RRF by hand, weighting the vector branch 100× more than the text branch:

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($ranks) -> {
        RETURN   1.0 / (60 + COALESCE($ranks[0], 100000))
             + 100.0 / (60 + COALESCE($ranks[1], 100000));
    } AS RankLambda)
LIMIT 10;
```

Custom `ScoreLambda` — combine the raw fulltext relevance and the negated vector distance (closer = larger score):

```yql
$queryText = "machine learning";
$queryVector = Knn::ToBinaryStringFloat([0.1, 0.2, 0.3, 0.4]);

SELECT id, title
FROM documents
ORDER BY HybridRank(
    FullTextScore(text, $queryText),
    Knn::CosineDistance(embedding, $queryVector),
    ($scores) -> {
        RETURN COALESCE($scores[0], 0.0) - COALESCE($scores[1], 1000000.0);
    } AS ScoreLambda)
LIMIT 10;
```

## Limitations {#limitations}

* `HybridRank(...)` must be the entire `ORDER BY` key; it cannot be negated, nested in a larger expression, or combined with other sort keys.
* The current implementation supports only tables with a single-column primary key.
* At least two scoring arguments are required — a single branch is not a hybrid query.
* `LIMIT` must be a literal (it sizes the per-branch candidate pools). Use an explicit `AS Limits` to allow a parameterized `LIMIT`.
* A custom fusion lambda (`RankLambda` or `ScoreLambda`) replaces the built-in fusion and cannot be combined with `Mode`, `Weights`, `K`, or `Normalize`. At most one of `RankLambda` or `ScoreLambda` may be specified.
* Prefixed indexes require equality predicates on every prefix column; partial prefixes are not supported by the current hybrid rewrite.
