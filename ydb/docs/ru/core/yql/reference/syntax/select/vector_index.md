# VIEW (Векторный индекс)

{% if oss == true and backend_name == "YDB" %}

{% note warning %}

{% include [OLAP_not_allow_text](../../../../_includes/not_allow_for_olap_text.md) %}

{% include [limitations](../../../../_includes/vector_index_limitations.md) %}

{% endnote %}

{% endif %}

Для выполнения запроса `SELECT` с использованием [векторного индекса](../../../../concepts/glossary.md#vector-index) в строчно-ориентированной таблице используйте следующий синтаксис:

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

{% note info %}

Векторный индекс поддерживает функцию расстояния или сходства [расширения Knn](../../udf/list/knn#functions-distance), выбранную при создании индекса.

Векторный индекс не будет автоматически выбран [оптимизатором](../../../../concepts/glossary.md#optimizer), поэтому его нужно указывать явно с помощью выражения `VIEW IndexName`.

{% endnote %}

## KMeansTreeSearchTopSize

Векторный поиск по индексу основан на приближённом алгоритме (ANN, Approximate Nearest Neighbors). Это значит, что результат поиска по векторному индексу может отличаться от результата поиска при полном сканировании таблицы.

Полнота поиска по индексу может быть отрегулирована параметром: `PRAGMA ydb.KMeansTreeSearchTopSize`.

Данный параметр задаёт максимальное число сканируемых кластеров, ближайших к запрашиваемому вектору, на каждом уровне дерева поиска.
Необходимо явно задавать значение данного параметра для каждого запроса.

Значение по умолчанию - 1. То есть, по умолчанию сканируется только 1 ближайший кластер на каждом уровне дерева поиска. Такое значение оптимально с точки зрения производительности и будет достаточным для векторов, близких к центру какого-либо кластера. Однако для векторов, примерно одинаково близких к нескольким кластерам, значения 1 не достаточно. Для увеличения полноты поиска (ценой некоторого замедления) следует увеличить значение PRAGMA, например:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize="10";
SELECT *
    FROM TableName VIEW IndexName
    ORDER BY Knn::CosineDistance(embedding, $target)
    LIMIT 10
```

Логика работы и настройка векторного индекса подробно описаны в отдельной
[статье](../../../../dev/vector-indexes-kmeans-tree-type.md).

## Примеры

* Выбор всех полей из таблицы `series` с использованием векторного индекса `views_index`, созданного для `embedding` с мерой близости "косинусное расстояние":

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Выбор всех полей из таблицы `series` с использованием векторного индекса с фильтрацией `views_filtered_index`, созданного для `embedding` с мерой близости "косинусное расстояние" и с ускорением фильтрации по колонке `release_date`:

  ```yql
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_filtered_index
      WHERE release_date = "2025-03-31"
      ORDER BY similarity DESC
      LIMIT 10
  ```
