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
$TargetEmbedding = [1.2f, 2.3f, 3.4f, 4.5f]

SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(
    Knn::FromBinaryString(embedding),
    $TargetEmbedding
)
LIMIT 10
```

## Типы данных

В математике для хранения точек используется вектор вещественных чисел.
В {{ ydb-short-name }} операции будут происходить над типом данных `List<Float>`, а храниться данные будут в строковом типе данных `String`.

## Функции

Функции работы с векторами реализовываются в виде пользовательских функций (UDF) в модуле `Knn`.

### Функции расстояния и сходства

Функции расстояния и сходства принимают на вход два списка вещественных чисел и возвращает расстояние/сходство между ними.

{% note info %}

Функции расстояния возвращает малое значение для близких векторов, функции сходства возвращают большие значения для близких векторов. Это следует учитывать в порядке сортировки.

{% endnote %}

Фукнции сходства:
* скалярное произведение `InnerProductSimilarity` (сумма произведений координат)
* косинусное сходство `CosineSimilarity` (скалярное произведение / длины векторов)

Фукнции расстояния:
* евклидово расстояние `EuclideanDistance` (корень от суммы квадратов разниц координат)
* косинусное расстояние `CosineDistance` ( 1 - косинусное сходство)

#### Сигнатуры функций

```sql
Knn::InnerProductSimilarity(List<Float>{Flags:AutoMap}, List<Float>{Flags:AutoMap})->Float?
Knn::CosineSimilarity(List<Float>{Flags:AutoMap}, List<Float>{Flags:AutoMap})->Float?
Knn::EuclideanDistance(List<Float>{Flags:AutoMap}, List<Float>{Flags:AutoMap})->Float?
Knn::CosineDistance(List<Float>{Flags:AutoMap}, List<Float>{Flags:AutoMap})->Float?
```

В случае ошибки вычисления, фукнции возвращают `NULL`.

### Функции преобразования вектора в бинарное представление

Функции преобразования нужны для сериализации множества вектора во внутреннее бинарное представление и обратно.
Бинарное представление вектора будет храниться в {{ ydb-short-name }} в типе `String`.

#### Сигнатуры функций

```sql
Knn::ToBinaryString(List<Float>{Flags:AutoMap})->String
Knn::FromBinaryString(String{Flags:AutoMap})->List<Float>?
```

## Примеры

### Создание таблицы

```sql
CREATE TABLE Facts (
    id Uint64,        // Id of fact
    user String,      // User name
    fact String,      // Human-readable description of a user fact
    embedding String, // Binary representation of embedding vector (result of Knn::ToBinaryString)
    PRIMARY KEY (id)
)
```

### Добавление векторов

```sql
UPSERT INTO Facts (id, user, fact, embedding) 
VALUES (123, "Williams", "Full name is John Williams", Knn::ToBinaryString([1.0f, 2.0f, 3.0f, 4.0f]))
```

### Точный поиск ближайших векторов

```sql
$TargetEmbedding = [1.2f, 2.3f, 3.4f, 4.5f]

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(
    Knn::FromBinaryString(embedding),
    $TargetEmbedding
)
LIMIT 10
```