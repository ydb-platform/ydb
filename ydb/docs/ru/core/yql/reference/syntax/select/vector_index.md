# VIEW (Векторный индекс)

Для выполнения запроса `SELECT` с использованием [векторного индекса](../../../../dev/vector-indexes.md) в строчно-ориентированной таблице используйте следующий синтаксис:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::DistanceFunction(...)
    LIMIT ...
```

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

SELECT ...
    FROM TableName VIEW IndexName
    WHERE ...
    ORDER BY Knn::SimilarityFunction(...) DESC
    LIMIT ...
```

Принципы работы и настройки векторного индекса подробно описаны в отдельной статье [{#T}](../../../../dev/vector-indexes.md).

{% note info %}

Векторный индекс поддерживает функцию расстояния или сходства [расширения Knn](../../udf/list/knn#functions-distance), выбранную при создании индекса.

Векторный индекс не будет автоматически выбран [оптимизатором](../../../../concepts/glossary.md#optimizer), поэтому его нужно указывать явно с помощью выражения `VIEW IndexName`.

Если не использовать выражение `VIEW`, запрос выполнит полное сканирование таблицы с попарным сравнением векторов. Рекомендуется проверять оптимальность написанного запроса, используя [анализ плана выполнения запроса](../../../../dev/optimization/plans.md). В частности, следует следить за отсутствием полного сканирования (full scan) основной таблицы.

{% endnote %}

{% note warning %}

{% include [limitations](../../../../_includes/vector-index-update-limitations.md) %}

{% endnote %}

## KMeansTreeSearchTopSize {#KMeansTreeSearchTopSize}

Векторный поиск по индексу основан на приближённом алгоритме (ANN, Approximate Nearest Neighbors). Это значит, что результат поиска по векторному индексу может отличаться от результата поиска при полном сканировании таблицы.

Полнота поиска по индексу может быть отрегулирована параметром: `PRAGMA ydb.KMeansTreeSearchTopSize`.

Данный параметр задаёт максимальное число сканируемых кластеров, ближайших к запрашиваемому вектору, на каждом уровне дерева поиска.

Задавайте значение явно при настройке баланса между полнотой поиска и стоимостью запроса.

Значение по умолчанию равно 4 для индекса с перекрывающимися кластерами (`overlap_clusters > 1`) и 10 для индекса без перекрытия. При значении 1 на каждом уровне сканируется только один ближайший кластер: это уменьшает стоимость запроса за счёт снижения полноты. Увеличение значения позволяет исследовать больше кластеров и может повысить полноту, но требует больше чтений и сравнений векторов. Например:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize="10";

SELECT *
    FROM TableName VIEW IndexName
    ORDER BY Knn::CosineDistance(embedding, $target)
    LIMIT 10
```

## Фильтрация по префиксу векторного индекса {#filtering}

[Векторные индексы с фильтрацией](../../../../dev/vector-indexes.md#filtered) поддерживают условия равенства, `IN` и `OR` по колонкам префикса. Для индекса на `(user, embedding)` условие `WHERE user IN ("john", "jane")` выбирает несколько категорий. Поддерживается и начальная часть составного префикса: индекс на `(user, article_id, embedding)` допускает `WHERE user = "john"`.

Итоговые `ORDER BY` и `LIMIT` применяются к объединённым кандидатам. Число исследуемых кластеров масштабируется с количеством подходящих групп префикса.

## Примеры

* Выбор всех полей из таблицы `series` с использованием векторного индекса `views_index`, созданного для `embedding` с мерой близости "косинусное расстояние":

  ```yql
  PRAGMA ydb.KMeansTreeSearchTopSize="10";
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_index
      ORDER BY similarity DESC
      LIMIT 10
  ```

* Выбор всех полей из таблицы `series` с использованием векторного индекса с фильтрацией `views_filtered_index`, созданного для `embedding` с мерой близости "косинусное расстояние" и с ускорением фильтрации по колонке `release_date`:

  ```yql
  PRAGMA ydb.KMeansTreeSearchTopSize="10";
  SELECT series_id, title, info, release_date, views, uploaded_user_id, Knn::CosineSimilarity(embedding, $target) as similarity
      FROM series VIEW views_filtered_index
      WHERE release_date = "2025-03-31"
      ORDER BY similarity DESC
      LIMIT 10
  ```
