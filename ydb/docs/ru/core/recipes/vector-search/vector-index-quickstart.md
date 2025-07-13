# Векторный индекс — быстрый старт

Эта статья поможет быстро начать работу с векторными индексами в {{ ydb-short-name }} на простейшем модельном примере.

В статье будут рассмотрены следующие шаги работы с векторным индексом:

* [создание таблицы с векторами](#step1);
* [заполнение таблицы данными](#step2);
* [построение векторного индекса](#step3);
* [выполнение векторного поиска без индекса](#step4);
* [выполнение векторного поиска с индексом](#step5).

## Шаг 1. Создание таблицы с векторами {#step1}

Сначала нужно создать таблицу в {{ ydb-short-name }}, в которой будут храниться векторы. Это можно сделать с помощью SQL-запроса:

```yql
CREATE TABLE Vectors (
  id Uint64,
  embedding String,
  PRIMARY KEY (id)
);
```

Эта таблица `Vectors` имеет два столбца:

- `id` — уникальный идентификатор каждого вектора;
- `embedding` — вектор вещественных чисел, [упакованный в строку](../../yql/reference/udf/list/knn.md#functions-convert).

## Шаг 2. Заполнение таблицы данными {#step2}

После создания таблицы следует добавить в нее векторы запросом `UPSERT INTO`:

```yql
UPSERT INTO Vectors(id, embedding)
VALUES 
    (1, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.f]), "FloatVector")),
    (2, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.25f]), "FloatVector")),
    (3, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.5f]), "FloatVector")),
    (4, Untag(Knn::ToBinaryStringFloat([-1.f, -1.f, -1.f, -1.f, -1.f]), "FloatVector")),
    (5, Untag(Knn::ToBinaryStringFloat([-2.f, -2.f, -2.f, -2.f, -4.f]), "FloatVector")),
    (6, Untag(Knn::ToBinaryStringFloat([-3.f, -3.f, -3.f, -3.f, -6.f]), "FloatVector"));
```

Описание функции `Knn::ToBinaryStringFloat` см. [здесь](../../yql/reference/udf/list/knn.md).

## Шаг 3. Построение векторного индекса {#step3}

Для создания векторного индекса `EmbeddingIndex` на таблице `Vectors` нужно использовать команду:

```yql
ALTER TABLE Vectors
ADD INDEX EmbeddingIndex
GLOBAL USING vector_kmeans_tree 
ON (embedding)
WITH (
  distance=cosine, 
  vector_type="float", 
  vector_dimension=5, 
  levels=1, 
  clusters=2)
```

Данная команда создаёт индекс типа `vector_kmeans_tree`. Подробнее об индексах такого типа вы можете прочитать [здесь](../../dev/vector-indexes.md#kmeans-tree-type). В данном модельном примере указан параметр `clusters=2` (разбиение множества векторов при построении индекса на два кластера на каждом уровне); для реальных данных рекомендованы значения в диапазоне от 64 до 512.

Общую информацию о векторных индексах, параметрах их создания и текущих ограничениях см. в разделе [{#T}](../../dev/vector-indexes.md).

## Шаг 4. Поиск в таблице без использования векторного индекса {#step4}

На данном шаге выполняется точный поиск 3-х ближайших соседей для заданного вектора `[1.f, 1.f, 1.f, 1.f, 4.f]` **без** использования индекса.

Сначала целевой вектор кодируется в бинарное представление с помощью [`Knn::ToBinaryStringFloat`](../../yql/reference/udf/list/knn#functions-convert).

Затем вычисляется косинусное расстояние от `embedding` каждой строки до целевого вектора.

Записи сортируются по возрастанию расстояния, и выбираются три (`$K`) первых записей, которые являются ближайшими.

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 4.f]);

SELECT id, Knn::CosineDistance(embedding, $TargetEmbedding) As CosineDistance
FROM Vectors
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

Результат выполнения запроса:

```bash
id CosineDistance
3  0.1055728197
2  0.1467181444
1  0.1999999881
```

Подробную информацию о точном векторном поиске без использования векторных индексов см. [здесь](../../yql/reference/udf/list/knn.md).

## Шаг 5. Поиск в таблице с использованием векторного индекса {#step5}

Для поиска 3-х ближайших соседей вектора `[1.f, 1.f, 1.f, 1.f, 4.f]` с использованием индекса `EmbeddingIndex`, который был создан на [шаге 3](#step3), нужно выполнить запрос:

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 4.f]);

SELECT id, Knn::CosineDistance(embedding, $TargetEmbedding) As CosineDistance
FROM Vectors VIEW EmbeddingIndex
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

Обратите внимание, что в данном запросе после названия таблицы указано, что отбор записей в ней следует производить с помощью векторного индекса: `FROM Vectors VIEW EmbeddingIndex`.

Результат выполнения запроса:

```bash
id CosineDistance
3  0.1055728197
2  0.1467181444
1  0.1999999881
```

Благодаря использованию индекса поиск ближайших векторов происходит значительно быстрее на больших выборках.

## Заключение

Данная статья приводит простой пример работы с векторным индексом: создание таблицы с векторами, заполнение таблицы векторами, построение векторного индекса для такой таблицы и поиск вектора в таблице с использованием векторного индекса или без него.

В случае маленькой таблицы, как в этом модельном примере, невозможно увидеть разницу в производительности запросов. Эти примеры призваны проиллюстрировать синтаксис при работе с векторными индексами. Более реалистичный пример с бо́льшим объёмом данных см. [здесь](vector-index-with-prepared-dataset.md).

Более подробную информацию о векторных индексах см. [здесь](../../dev/vector-indexes.md).