# KNN

## Введение {#introduction}

Одним из частных случаев {% if backend_name == 'YDB' %}[векторного поиска](../../../../concepts/query_execution/vector_search.md){% else %}векторного поиска{% endif %} является задача [k-NN](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), где требуется найти `k` ближайших точек к точке запроса. Это может быть полезно в различных приложениях, таких как классификация изображений, рекомендательные системы и многое другое.

Решения задачи k-NN разбивается на два крупных подкласса методов: точные и приближенные.

### Точный метод {#exact-method}

{% include [vector_search_exact.md](../../_includes/vector_search_exact.md) %}

### Приближенные методы  {#approximate-methods}

{% include [vector_search_approximate.md](../../_includes/vector_search_approximate.md) %}

{% note info %}

Рекомендуется проводить замеры, обеспечивает ли такое преобразование достаточную точность/полноту.

{% endnote %}

## Типы данных {#data-types}

В математике для хранения точек используется вектор вещественных или целых чисел.
В этом модуле вектора представлены типом данных `String`, который является бинарным сериализованным представлением вектора.

## Функции {#functions}

Функции работы с векторами реализовываются в виде пользовательских функций (UDF) в модуле `Knn`.

### Функции преобразования вектора в бинарное представление {#functions-convert}

Функции преобразования нужны для сериализации векторов во внутреннее бинарное представление и обратно.

Все функции сериализации упаковывают возвращаемые данные типа `String` в [Tagged](../../types/special.md) тип.

{% if backend_name == "YDB" %}
Бинарное представление вектора можно сохранить в {{ ydb-short-name }} колонку.

{% note info %}

В настоящий момент {{ ydb-short-name }} не поддерживает хранение `Tagged` типов и поэтому перед сохранением бинарного представления векторов нужно извлечь `String` с помощью функции [Untag](../../builtins/basic#as-tagged).

{% endnote %}

{% note info %}

В настоящий момент {{ ydb-short-name }} не поддерживает построение векторных индексов для бинарных векторов `BitVector`.

{% endnote %}

{% endif %}

#### Сигнатуры функций {#functions-convert-signature}

```yql
Knn::ToBinaryStringFloat(List<Float>{Flags:AutoMap})->Tagged<String, "FloatVector">
Knn::ToBinaryStringUint8(List<Uint8>{Flags:AutoMap})->Tagged<String, "Uint8Vector">
Knn::ToBinaryStringInt8(List<Int8>{Flags:AutoMap})->Tagged<String, "Int8Vector">
Knn::ToBinaryStringBit(List<Double>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Float>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Uint8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Int8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::FloatFromBinaryString(String{Flags:AutoMap})->List<Float>?
```

#### Формат сериализации {#functions-convert-format}

Функции сериализации векторных данных преобразуют массив элементов в байтовую строку следующего формата:

- **Основная часть** — непрерывный массив элементов ([knn-serializer.h](https://github.com/ydb-platform/ydb/blob/0b506f56e399e0b4e6a6a4267799da68a3164bf7/ydb/library/yql/udfs/common/knn/knn-serializer.h#L19))
- **Тип** — 1 байт в конце строки, обозначающий тип данных ([knn-defines.h](https://github.com/ydb-platform/ydb/blob/24026648dd7463d58e1470aa8981b17677116e7c/ydb/library/yql/udfs/common/knn/knn-defines.h#L5)):  
  `1` — `Float` (4 байта на элемент);  
  `2` — `Uint8` (1 байт на элемент);
  `3` — `Int8` (1 байт на элемент);
  `10` — `Bit` (1 бит на элемент).  

Например, вектор из 5 элементов типа `Float` сериализуется в строку длиной 21 байт: 4 байта × 5 элементов (основная часть) + 1 байт (тип) = 21 байт.

#### Детали имплементации {#functions-convert-details}

`ToBinaryStringBit` преобразует в `1` все координаты, которые больше `0`. Остальные координаты преобразуются в `0`.

### Функции расстояния и сходства {#functions-distance}

Функции расстояния и сходства принимают на вход два вектора и возвращают расстояние/сходство между ними.

{% note info %}

Функции расстояния возвращают малое значение для близких векторов, функции сходства возвращают большие значения для близких векторов. Это следует учитывать в порядке сортировки.

{% endnote %}

Функции сходства:

* скалярное произведение `InnerProductSimilarity` (сумма произведений координат)
* косинусное сходство `CosineSimilarity` (скалярное произведение разделенное на произведение длин векторов)

Функции расстояния:

* косинусное расстояние `CosineDistance` (1 - косинусное сходство)
* манхэттенское расстояние `ManhattanDistance`, также известно как `L1 distance`  (сумма модулей покоординатной разности)
* Евклидово расстояние `EuclideanDistance`, также известно как `L2 distance` (корень суммы квадратов покоординатной разности)

#### Сигнатуры функций {#functions-distance-signatures}

```yql
Knn::InnerProductSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::ManhattanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::EuclideanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
```

В случае отличающихся длины или формата, эти функции возвращают `NULL`.

{% note info %}

Все функции расстояния и сходства поддерживают перегрузки с аргументами одного из типов `Tagged<String, "FloatVector">`, `Tagged<String, "Uint8Vector">`, `Tagged<String, "Int8Vector">`, `Tagged<String, "BitVector">`.

Если оба аргумента `Tagged`, то значение тега должно совпадать, иначе запрос завершится с ошибкой.

Пример:

```text
Error: Failed to find UDF function: Knn.CosineDistance, reason: Error: Module: Knn, function: CosineDistance, error: Arguments should have same tags, but 'FloatVector' is not equal to 'Uint8Vector'
```

{% endnote %}

## Примеры точного поиска {#exact-vector-search-examples}

{% if backend_name == "YDB" %}

### Создание таблицы {#exact-vector-search-examples-create}

```yql
CREATE TABLE Facts (
    id Uint64,        -- Id of fact
    user Utf8,        -- User name
    fact Utf8,        -- Human-readable description of a user fact
    embedding String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    PRIMARY KEY (id)
);
```

### Добавление векторов {#exact-vector-search-examples-upsert}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));
```

{% else %}

### Декларация данных {#exact-vector-search-examples-create-list}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
$facts = AsList(
    AsStruct(
        123 AS id,  -- Id of fact
        "Williams" AS user,  -- User name
        "Full name is John Williams" AS fact,  -- Human-readable description of a user fact
        Knn::ToBinaryStringFloat($vector) AS embedding,  -- Binary representation of embedding vector
    ),
);
```

{% endif %}

### Точный поиск K ближайших векторов {#exact-vector-search-k-nearest}

{% if backend_name == "YDB" %}

```yql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

{% else %}

```yql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM AS_TABLE($facts)
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

{% endif %}

### Точный поиск векторов, находящихся в радиусе R {#exact-vector-search-radius}

{% if backend_name == "YDB" %}

```yql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

{% else %}

```yql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM AS_TABLE($facts)
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

{% endif %}

## Примеры приближенного поиска {#approximate-vector-search-examples}

Данный пример отличается от [примера с точным поиском](#exact-vector-search-examples) использованием битового квантования.
Это позволяет сначала делать грубый предварительный поиск по колонке `embedding_bit`, а затем уточнять результаты по основной колонке с векторами `embedding`.

{% if backend_name == "YDB" %}

### Создание таблицы {#approximate-vector-search-examples-create}

```yql
CREATE TABLE Facts (
    id Uint64,            -- Id of fact
    user Utf8,            -- User name
    fact Utf8,            -- Human-readable description of a user fact
    embedding String,     -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    embedding_bit String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringBit)
    PRIMARY KEY (id)
);
```

### Добавление векторов {#approximate-vector-search-examples-upsert}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding, embedding_bit)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"), Untag(Knn::ToBinaryStringBit($vector), "BitVector"));
```

{% else %}

### Декларация данных {#approximate-vector-search-examples-create-list}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
$facts = AsList(
    AsStruct(
        123 AS id,  -- Id of fact
        "Williams" AS user,  -- User name
        "Full name is John Williams" AS fact,  -- Human-readable description of a user fact
        Knn::ToBinaryStringFloat($vector) AS embedding,  -- Binary representation of embedding vector
        Knn::ToBinaryStringBit($vector) AS embedding_bit,  -- Binary representation of embedding vector
    ),
);
```

{% endif %}

### Скалярное квантование {#approximate-vector-search-scalar-quantization}

ML модель может выполнять квантование или это можно сделать вручную с помощью YQL.

Ниже приведен пример квантования в YQL.

#### Float -> Int8 {#approximate-vector-search-scalar-quantization-map}

```yql
$MapInt8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $range = $max - $min;
  RETURN CAST(Math::Round(IF($x < $min, -127, IF($x > $max, 127, ($x / $range) * 255))) As Int8)
};

$FloatList = [-1.2f, 2.3f, 3.4f, -4.7f];
SELECT ListMap($FloatList, $MapInt8);
```

### Приближенный поиск K ближайших векторов: битовое квантование {#approximate-vector-search-scalar-quantization-example}

Алгоритм приближенного поиска:

* производится приближенный поиск с использованием битового квантования;
* получается приближенный список векторов;
* в этом списке производим поиск без использования квантования.

{% if backend_name == "YDB" %}

```yql
$K = 10;
$Target = [1.2f, 2.3f, 3.4f, 4.5f];
$TargetEmbeddingBit = Knn::ToBinaryStringBit($Target);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);

$Ids = SELECT id FROM Facts
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT $K * 10;

SELECT * FROM Facts
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT $K;
```

{% else %}

```yql
$K = 10;
$Target = [1.2f, 2.3f, 3.4f, 4.5f];
$TargetEmbeddingBit = Knn::ToBinaryStringBit($Target);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);

$Ids = SELECT id FROM AS_TABLE($facts)
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT $K * 10;

SELECT * FROM AS_TABLE($facts)
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT $K;
```

{% endif %}
