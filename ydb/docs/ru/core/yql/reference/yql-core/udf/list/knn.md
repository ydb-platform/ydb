# KNN
## Введение

[Поиск ближайшего соседа](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) - это задача оптимизации, заключающаяся в нахождении ближайшей точки (или набора точек) в заданном наборе данных к заданной точке запроса. Близость может быть определена в терминах метрики расстояния или сходства.
Обобщением задачи NN является задача [k-NN](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), где от нас требуется найти k ближайших точек к точке запроса. Это может быть полезно в различных приложениях, таких как классификация изображений, рекомендательные системы и многое другое.

Решения задачи k-NN разбивается на два крупных подкласса методов: точные и приближенные. В данном документе речь пойдет о точном подходе, а именно о точном подходе методом грубой силы.

В основе метода лежит вычисление расстояния от точки запроса до каждой другой точки в базе данных. Этот алгоритм, также известный как наивный подход, имеет время выполнения `O(dn)`, где `n` - количество точек в наборе данных, а `d` - его размерность.

Преимуществом метода является отсутствие необходимости в дополнительных структурах данных, вроде специализированных векторных индексов.
Недостатком является необходимость полного перебора данных. Но данный недостаток является несущественным в случаях, когда произошла предварительная фильтрация данных, например, по идентификатору пользователя.

Пример:

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10
```

## Типы данных

В математике для хранения точек используется вектор вещественных чисел.
В {{ ydb-short-name }} операции будут происходить над строковым типом данных `String`, который является бинарным сериализованным представлением `List<Float>`.

## Функции

Функции работы с векторами реализовываются в виде пользовательских функций (UDF) в модуле `Knn`.

### Функции расстояния и сходства

Функции расстояния и сходства принимают на вход два списка вещественных чисел и возвращает расстояние/сходство между ними.

{% note info %}

Функции расстояния возвращает малое значение для близких векторов, функции сходства возвращают большие значения для близких векторов. Это следует учитывать в порядке сортировки.

{% endnote %}

Фукнции сходства:
* скалярное произведение `InnerProductSimilarity` (сумма произведений координат)
* косинусное сходство `CosineSimilarity` (скалярное произведение / произведение длин векторов)

Фукнции расстояния:
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

В случае когда у аргументов разная длинна или разный формат, фукнции возвращают `NULL`.

{% note info %}

Также все функции расстояния и сходства поддерживают такие перегрузки, когда первый или второй аргумент могут быть `Tagged<String, "FloatVector">`, `Tagged<String, "Uint8Vector">`, `Tagged<String, "Int8Vector">`, `Tagged<String, "BitVector">`.

Если оба аргумента `Tagged`, то значение тега должно совпадать, иначе запрос завершится с ошибкой.

{% endnote %}


### Функции преобразования вектора в бинарное представление

Функции преобразования нужны для сериализации множества вектора во внутреннее бинарное представление и обратно.
Бинарное представление вектора будет храниться в {{ ydb-short-name }} в типе `String`.

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

* `ToBinaryStringBit` преобразует в `1` все коордианты которые больше `0`, остальные координаты преобразуются в `0`.

## Примеры

### Создание таблицы

```sql
CREATE TABLE Facts (
    id Uint64,        // Id of fact
    user Utf8,        // User name
    fact Utf8,        // Human-readable description of a user fact
    embedding String, // Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    PRIMARY KEY (id)
)
```

### Добавление векторов

```sql
UPSERT INTO Facts (id, user, fact, embedding) 
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat(CAST([1, 2, 3, 4] AS List<Float>), "FloatVector")))
```

{% note info %}

{{ ydb-short-name }} не поддерживает хранение `Tagged` типов, поэтому нужно сохранять данные векторов как `String` и при вставке использовать функцию `Untag`

{% endnote %}

### Точный поиск ближайших векторов

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10
```

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < 0.1
```

### Пример неточного поиска ближайших векторов: квантизация

```sql
CREATE TABLE Facts (
    id Uint64 NOT NULL,
    embedding String,
    embedding_bit String,
    PRIMARY KEY (id)
);

INSERT INTO my_table VALUES(
    (1, Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]), Knn::ToBinaryStringBit([1.2f, 2.3f, 3.4f, 4.5f]))
);
```

```sql
$TargetEmbeddingBit = Knn::ToBinaryStringBit([1.2f, 2.3f, 3.4f, 4.5f]);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

$Ids = SELECT id FROM Facts
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT 100;

SELECT * FROM Facts
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT 10
```
