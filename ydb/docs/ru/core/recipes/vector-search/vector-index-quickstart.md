# Векторный индекс — быстрый старт

Эта статья поможет вам быстро начать работу с векторными индексами в YDB на простейшем модельном примере.

В статье мы рассмотрим основные шаги:

* создание таблицы, содержащей векторы;

* заполнение таблицы (включая колонку с векторами);

* построение векторного индекса;

* выполнение векторного поиска без индекса;

* выполнение векторного поиска с индексом.

## Шаг 1. Создание таблицы {step1}

Первым шагом необходимо создать таблицу в YDB, в которой будут храниться ваши векторы.

Для создания таблицы выполните следующий SQL-запрос:

```yql
CREATE TABLE Vectors (
  id Uint64,
  embedding String,
  PRIMARY KEY (id)
);
```

Эта таблица `Vectors` имеет два столбца: `id` (уникальный идентификатор каждого вектора) и `embedding` (сюда мы будем записывать вектора вещественных чисел, сконвертированные в строки).

## Шаг 2. Заполнение таблицы данными {step2}

После создания таблицы добавьте в нее векторы. Используйте команду `UPSERT INTO` для добавления явно заданных шести векторов:

```yql
$vector = [1.f, 1.f, 1.f, 1.f, 1.f];
UPSERT INTO Vectors (id, embedding)
VALUES (1, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));

$vector = [1.f, 1.f, 1.f, 1.f, 1.25f];
UPSERT INTO Vectors (id, embedding)
VALUES (2, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));

$vector = [1.f, 1.f, 1.f, 1.f, 1.5f];
UPSERT INTO Vectors (id, embedding)
VALUES (3, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));

$vector = [-1.f, -1.f, -1.f, -1.f, -1.f];
UPSERT INTO Vectors (id, embedding)
VALUES (4, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));

$vector = [-2.f, -2.f, -2.f, -2.f, -4.f];
UPSERT INTO Vectors (id, embedding)
VALUES (5, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));

$vector = [-3.f, -3.f, -3.f, -3.f, -6.f];
UPSERT INTO Vectors (id, embedding)
VALUES (6, Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));
```

## Шаг 3. Построение векторного индекса {step3}

Создайте векторный индекс на таблице `Vectors`. Используйте следующую команду:

```yql
ALTER TABLE Vectors
ADD INDEX Index
GLOBAL USING vector_kmeans_tree 
ON (embedding)
WITH (distance=cosine, vector_type="float", vector_dimension=5, levels=1, clusters=2)
```

Данная команда создает индекс типа `vector_kmeans_tree`, подробнее о векторах такого типа вы можете прочитать [здесь](../../dev/vector-indexes?version=main#kmeans-tree-type). В данном модельном примере указан параметр `clusters=2` (разбивать множество векторов при построении индекса на два кластера на каждом уровне), для реальных данных рекомендованы значения `64-512`.

Общую информацию о векторных индексах, параметрах их создания и текущих ограничениях см. в разделе [Векторные индексы](../../dev/vector-indexes?version=main).

## Шаг 4. Поиск в таблице без использования векторного индекса {step4}

Выполните поиск ближайших 3-х соседей вектора `[1.f, 1.f, 1.f, 1.f, 4.f]` без использования индекса:

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

Подробную информацию о точном векторном поиске без использования векторных индексов см. [тут](../../yql/reference/udf/list/knn?version=main).

## Шаг 5. Поиск в таблице с использованием векторного индекса {step5}

Теперь выполните поиск ближайших соседей вектора `[1.f, 1.f, 1.f, 1.f, 4.f]` с использованием индекса `Index`, который вы создали на [шаге 3](#шаг-3-построение-векторного-индекса-step3):

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 4.f]);

SELECT id, Knn::CosineDistance(embedding, $TargetEmbedding) As CosineDistance
FROM Vectors VIEW Index
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

## Заключение

Теперь вы можете создавать таблицы с векторами, строить для таких таблиц векторные индексы и выполнять поиск на таких таблицах с использованием векторного индекса или без него.

В случае маленькой таблицы, как в нашем модельном примере, вы не увидите большой разницы в производительности запросов. Эти примеры призваны проиллюстрировать синтаксис при работе с векторными индексами. Более реалистичный пример с бОльшим объемом данных см. [тут](vector-index-with-prepared-dataset.md).

Для получения более подробной информации посетите страницу о [векторных индексах](../../dev/vector-indexes?version=main).

Теперь вы готовы эффективно использовать векторные индексы в YDB для оптимизации поиска ближайших соседей!