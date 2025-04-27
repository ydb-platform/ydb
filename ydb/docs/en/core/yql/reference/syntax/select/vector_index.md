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

The vector index will not be automatically selected by the [optimizer](../../../../concepts/glossary.md#optimizer), so it must be specified explicitly using the expression `VIEW IndexName'.

{% endnote %}

## Examples

* Select all the fields from the `series` row-oriented table using the `views_index` vector index created for `embedding` and inner product similarity:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::InnerProductSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Select all the fields from the `series` row-oriented table using the `views_index2` prefixed vector index created for `embedding` and inner product similarity with prefix column `release_date`:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::InnerProductSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index2
      WHERE release_date = "2025-03-31"
      ORDER BY Knn::InnerProductSimilarity(embedding, $TargetEmbedding) DESC
      LIMIT 10
  ```
