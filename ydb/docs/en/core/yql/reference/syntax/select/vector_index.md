# VIEW (Vector index)

To select data from a row-oriented table using a [vector index](../../../../dev/vector-indexes.md), use the following statements:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SomeDistance(...)
    LIMIT ...
```

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SomeSimilarity(...) DESC
    LIMIT ...
```

Principles of operation and settings of the vector index are described in detail in [{#T}](../../../../dev/vector-indexes.md).

{% note info %}

A vector index supports a distance or similarity function [from the Knn extension](../../udf/list/knn#functions-distance) specified during its construction.

A vector index isn't automatically selected by the [optimizer](../../../../concepts/glossary.md#optimizer) and must be specified explicitly using the `VIEW IndexName` expression.

If the `VIEW` expression is not used, the query will perform a full table scan with pairwise comparison of vectors. It is recommended to check the optimality of the written query using [query plan analysis](../../../../dev/optimization/plans.md). In particular, ensure there is no full scan of the main table.

{% endnote %}

{% note warning %}

{% include [limitations](../../../../_includes/vector-index-update-limitations.md) %}

{% endnote %}

## KMeansTreeSearchTopSize {#KMeansTreeSearchTopSize}

Indexed vector search is based on an approximate algorithm (ANN, Approximate Nearest Neighbors). That means that indexed search may produce a result that differs from a similar full-scan nearest neighbor search.

Completeness of the indexed vector search is controlled by the following parameter: `PRAGMA ydb.KMeansTreeSearchTopSize`.

This parameter controls the maximum number of scanned clusters nearest to the requested search vector at every level of the search tree.
Set the parameter explicitly when tuning the balance between recall and query cost.

The default value is 4 for an index with overlapping clusters (`overlap_clusters > 1`) and 10 for an index without overlap. The value 1 scans only one nearest cluster at each level and reduces query cost at the expense of recall. Increasing the value explores more clusters and can improve recall, but requires more reads and vector comparisons. For example:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize="10";

SELECT *
    FROM TableName VIEW IndexName
    ORDER BY Knn::CosineDistance(embedding, $target)
    LIMIT 10
```

## Filtering with a prefixed vector index {#filtering}

[Filtered vector indexes](../../../../dev/vector-indexes.md#filtered) support equality, `IN`, and `OR` predicates on prefix columns. For an index on `(user, embedding)`, use `WHERE user IN ("john", "jane")` to search several categories. A leading part of a multi-column prefix is also supported: an index on `(user, article_id, embedding)` accepts `WHERE user = "john"`.

The final `ORDER BY` and `LIMIT` apply to the combined candidates. The cluster search budget scales with the number of matching prefix groups.

## Examples

* Select all the fields from the `series` row-oriented table using the `views_index` vector index created for `embedding` and cosine similarity:

  ```yql
  PRAGMA ydb.KMeansTreeSearchTopSize="10";
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Select all the fields from the `series` row-oriented table using the `views_filtered_index` filtered vector index created for `embedding` and optimized for efficient filtering by `release_date`:

  ```yql
  PRAGMA ydb.KMeansTreeSearchTopSize="10";
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_filtered_index
      WHERE release_date = "2025-03-31"
      ORDER BY similarity DESC
      LIMIT 10
  ```
