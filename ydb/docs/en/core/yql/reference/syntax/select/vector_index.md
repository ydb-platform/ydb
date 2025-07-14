# VIEW (Vector index)

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

{% include [limitations](../../../../_includes/vector_index_limitations.md) %}

{% endnote %}

{% endif %}

To select data from a row-oriented table using a [vector index](../../../../concepts/glossary.md#vector-index), use the following statements:

```yql
SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SomeDistance(...)
    LIMIT ...
```

```yql
SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SomeSimilarity(...) DESC
    LIMIT ...
```

{% note info %}

A vector index supports a distance or similarity function [from the Knn extension](../../udf/list/knn#functions-distance) specified during its construction.

A vector index isn't automatically selected by the [optimizer](../../../../concepts/glossary.md#optimizer) and must be specified explicitly using the `VIEW IndexName` expression.

{% endnote %}

## KMeansTreeSearchTopSize

Indexed vector search is based on an approximate algorithm (ANN, Approximate Nearest Neighbors). That means that indexed search may produce a result that differs from a similar full-scan nearest neighbor search.

Completeness of the indexed vector search is controlled by the following parameter: `PRAGMA ydb.KMeansTreeSearchTopSize`.

This parameter controls the maximum number of scanned clusters nearest to the requested search vector at every level of the search tree.
The parameter should be set explicitly for every search query.

The default value is 1. This means that only one nearest cluster is scanned at every level of the search tree by default. This parameter value maximizes search performance and results in good search quality for vectors near to the center of a cluster. But this value may be insufficient for vectors that are about equally close to multiple clusters. So, to increase the search quality for such vectors (at the expense of slightly reduced search performance), you should increase the PRAGMA value, for example:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize="10";
SELECT *
    FROM TableName VIEW IndexName
    ORDER BY Knn::CosineDistance(embedding, $target)
    LIMIT 10
```

## Examples

* Select all the fields from the `series` row-oriented table using the `views_index` vector index created for `embedding` and cosine similarity:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Select all the fields from the `series` row-oriented table using the `views_index2` prefixed vector index created for `embedding` and cosine similarity with prefix column `release_date`:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index2
      WHERE release_date = "2025-03-31"
      ORDER BY similarity DESC
      LIMIT 10
  ```
