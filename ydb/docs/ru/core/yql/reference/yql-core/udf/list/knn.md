# KNN
## Введение

[Поиск ближайшего соседа](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) - это задача оптимизации, заключающаяся в нахождении ближайшей точки (или набора точек) в заданном наборе данных к заданной точке запроса. Близость может быть определена в терминах метрики расстояния или сходства.
Обобщением задачи NN является задача [k-NN](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), где от нас требуется найти `k` ближайших точек к точке запроса. Это может быть полезно в различных приложениях, таких как классификация изображений, рекомендательные системы и многое другое.

Решения задачи k-NN разбивается на два крупных подкласса методов: точные и приближенные.

### Точный метод

В основе точного метода лежит вычисление расстояния от точки запроса до каждой другой точки в базе данных. Этот алгоритм, также известный как наивный подход, имеет время выполнения `O(dn)`, где `n` - количество точек в наборе данных, а `d` - его размерность.

Преимуществом метода является отсутствие необходимости в дополнительных структурах данных, вроде специализированных векторных индексов.
Недостатком является необходимость полного перебора данных. Но данный недостаток является несущественным в случаях, когда произошла предварительная фильтрация данных, например, по идентификатору пользователя.

Пример:

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10;
```

### Приближенные методы

Приближенные методы не производят полный перебор исходных данных, за счет этого работают существенно быстрее, хоть и допускают некоторое ухудшение качества поиска.

В данном документе приведен [пример приближенного поиска](#примеры-приближенного-поиска) с помощью скалярного квантования, не требущий построения вторичного векторного индекса.

**Скалярное квантование** это метод сжатия векторов, когда множество координат отображаются в множество меньшей размерности.
{{ ydb-short-name }} поддерживает точный поиск по `Float`, `Int8`, `Uint8`, `Bit` векторам.
Соответственно, возможно скалярное квантование из `Float` в один из этих типов.

Скалярное квантование уменьшает время необходимое для чтения/записи, поскольку число байт сокращается в разы.
Например, при квантовании из `Float` в `Bit` каждый вектор становится меньше в `32` раза.

{% note info %}

Рекомендуется проводить замеры, обеспечивает ли такое преобразование достаточную точность/полноту.

{% endnote %}

## Типы данных

В математике для хранения точек используется вектор вещественных или целых чисел.
В {{ ydb-short-name }} вектора хранятся в строковом типе данных `String`, который является бинарным сериализованным представлением вектора.

## Функции

Функции работы с векторами реализовываются в виде пользовательских функций (UDF) в модуле `Knn`.

### Функции преобразования вектора в бинарное представление

Функции преобразования нужны для сериализации векторов во внутреннее бинарное представление и обратно.

Все функции сериализации упаковывают возвращаемые данные типа `String` в [Tagged](../../types/special.md) тип.

Бинарное представление вектора можно сохранить в {{ ydb-short-name }} колонку.
В настоящий момент {{ ydb-short-name }} не поддерживает хранение `Tagged` типов и поэтому перед сохранением бинарного представления векторов нужно извлечь `String` с помощью функции [Untag](../../builtins/basic#as-tagged).

#### Сигнатуры функций

```sql
Knn::ToBinaryStringFloat(List<Float>{Flags:AutoMap})->Tagged<String, "FloatVector">
Knn::ToBinaryStringUint8(List<Uint8>{Flags:AutoMap})->Tagged<String, "Uint8Vector">
Knn::ToBinaryStringInt8(List<Int8>{Flags:AutoMap})->Tagged<String, "Int8Vector">
Knn::ToBinaryStringBit(List<Double>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Float>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Uint8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Int8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::FloatFromBinaryString(String{Flags:AutoMap})->List<Float>?
```

#### Детали имплементации

`ToBinaryStringBit` преобразует в `1` все координаты которые больше `0`, остальные координаты преобразуются в `0`.

### Функции расстояния и сходства

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

#### Сигнатуры функций

```sql
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

```
Error: Failed to find UDF function: Knn.CosineDistance, reason: Error: Module: Knn, function: CosineDistance, error: Arguments should have same tags, but 'FloatVector' is not equal to 'Uint8Vector'
```

{% endnote %}

## Примеры точного поиска

### Создание таблицы

```sql
CREATE TABLE Facts (
    id Uint64,        -- Id of fact
    user Utf8,        -- User name
    fact Utf8,        -- Human-readable description of a user fact
    embedding String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    PRIMARY KEY (id)
);
```

### Добавление векторов

```sql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));
```

### Точный поиск K ближайших векторов

```sql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

### Точный поиск векторов, находящихся в радиусе R

```sql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

## Примеры приближенного поиска

Данный пример отличается от [примера с точным поиском](#примеры-точного-поиска) использованием битового квантования.
Это позволяет сначала делать грубый предварительный поиск по колонке `embedding_bit`, а затем уточнять результаты по основной колонке с векторами `embedding`.

### Создание таблицы

```sql
CREATE TABLE Facts (
    id Uint64,            -- Id of fact
    user Utf8,            -- User name
    fact Utf8,            -- Human-readable description of a user fact
    embedding String,     -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    embedding_bit String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringBit)
    PRIMARY KEY (id)
);
```

### Добавление векторов

```sql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding, embedding_bit)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"), Untag(Knn::ToBinaryStringBit($vector), "BitVector"));
```

### Скалярное квантование

ML модель может выполнять квантование или это можно сделать вручную с помощью YQL.

Ниже приведен пример квантования в YQL.

#### Float -> Int8

```sql
$MapInt8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $range = $max - $min;
  RETURN CAST(Math::Round(IF($x < $min, -127, IF($x > $max, 127, ($x / $range) * 255))) As Int8)
};

$FloatList = [-1.2f, 2.3f, 3.4f, -4.7f];
SELECT ListMap($FloatList, $MapInt8);
```

### Приближенный поиск K ближайших векторов: битовое квантование

Алгоритм приближенного поиска:
* производится приближенный поиск с использованием битового квантования;
* получается приближенный список векторов;
* в этом списке производим поиск без использования квантования.

```sql
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
