# VIEW (Векторный индекс)

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

{% endnote %}

{% endif %}

Для выполнения запроса `SELECT` с использованием векторного индекса в строчно-ориентированной таблице используйте следующий синтаксис:

```yql
SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::DistanceFunction(...)
    LIMIT ...
```

```yql
SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SimilarityFunction(...) DESC
    LIMIT ...
```


## Примеры

* Выбор всех полей из таблицы `series` с использованием векторного индекса `views_index`, созданного для `embedding` с мерой близости "скалярное произведение":  

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::InnerProductSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Выбор всех полей из таблицы `series` с использованием префиксного векторного индекса `views_index2`, созданного для `embedding` с мерой близости "скалярное произведение" и префиксной колонкой `release_date`:  

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::InnerProductSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index2
      WHERE release_date = "2025-03-31"
      ORDER BY Knn::InnerProductSimilarity(embedding, $TargetEmbedding) DESC
      LIMIT 10
  ```
