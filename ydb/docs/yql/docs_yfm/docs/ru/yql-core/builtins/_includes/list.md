# Функции для работы со списками

## ListCreate {#list-create}

Сконструировать пустой список. В единственном аргументе указывается строка с описанием типа данных ячейки списка, либо сам тип, полученный с помощью [предназначенных для этого функций](../types.md). Списков с неизвестным типом ячейки в YQL не бывает.

[Документация по формату описания типа](../../types/type_string.md).

**Примеры**
``` yql
SELECT ListCreate(Tuple<String,Double?>);
```

``` yql
SELECT ListCreate(OptionalType(DataType("String")));
```

## AsList и AsListStrict {#aslist}

Сконструировать список из одного или более аргументов. Типы аргументов должны быть совместимы в случае `AsList` и строго совпадать в случае `AsListStrict`.

**Примеры**
``` yql
SELECT AsList(1, 2, 3, 4, 5);
```

## ListLength {#listlength}

Количество элементов в списке.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListLength(list_column) FROM my_table;
```
{% endif %}
## ListHasItems

Проверка того, что список содержит хотя бы один элемент.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListHasItems(list_column) FROM my_table;
```
{% endif %}

## ListCollect {#listcollect}

Преобразовать ленивый список (строится, например, функциями [ListFilter](#listfilter), [ListMap](#listmap), [ListFlatMap](#listflatmap)) в энергичный. В отличие от ленивого списка, в котором каждый повторный проход заново вычисляет его содержимое, в энергичном списке содержимое списка строится сразу ценой большего потребления памяти.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListCollect(list_column) FROM my_table;
```
{% endif %}
## ListSort, ListSortAsc и ListSortDesc {#listsort}

Отсортировать список. По умолчанию выполняется сортировка по возрастанию (`ListSort` — алиас к `ListSortAsc`).

Аргументы:

1. Список;
2. Опциональное выражение для получения ключа сортировки из элемента списка (по умолчанию сам элемент).

**Примеры**

{% if feature_column_container_type %}
``` yql
SELECT ListSortDesc(list_column) FROM my_table;
```
{% endif %}
``` yql
$list = AsList(
    AsTuple("x", 3),
    AsTuple("xx", 1),
    AsTuple("a", 2)
);

SELECT ListSort($list, ($x) -> {
    RETURN $x.1;
});
```

{% note info %}

В примере использовалась [лямбда функция](../../syntax/expressions.md#lambda).

{% endnote %}

## ListExtend и ListExtendStrict {#listextend}

Последовательно соединить списки (конкатенация списков). В качестве аргументов могут быть списки, опциональные списки и `NULL`.
Типы элементов списков должны быть совместимы в случае `ListExtend` и строго совпадать в случае `ListExtendStrict`.
Если хотя бы один из списков является опциональным, то таким же является и результат.
Если хотя бы один аргумент является `NULL`, то тип результата - `NULL`.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListExtend(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```
{% endif %}

## ListUnionAll {#listunionall}

Последовательно соединить списки структур (конкатенация списков). В выходном списке структур будет присутствовать поле, если оно есть хотя бы в одном исходном списке, при этом в случае отсутствия такого поля в каком-либо списке оно дополняется как NULL. В случае, когда поле присутствует в двух и более списках, поле в результате приводит в общий тип.

Если хотя бы один из списков является опциональным, то таким же является и результат.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListUnionAll(
    list_column_1,
    list_column_2,
    list_column_3
) FROM my_table;
```
{% endif %}

## ListZip и ListZipAll {#listzip}

По входящим спискам построить список пар, содержащих соответствующие по индексу элементы списков (`List<Tuple<first_list_element_type,second_list_element_type>>`).

Длина возвращаемого списка определяется самым коротким списком для ListZip и самым длинным — для ListZipAll.
Когда более короткий список исчерпан, в качестве пары к элементам более длинного списка подставляется пустое значение (`NULL`) соответствующего [optional типа](../../types/optional.md).

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListZip(list_column_1, list_column_2, list_column_3),
    ListZipAll(list_column_1, list_column_2)
FROM my_table;
```
{% endif %}

## ListEnumerate {#listenumerate}

Построить список пар (Tuple), содержащих номер элемента и сам элемент (`List<Tuple<Uint64,list_element_type>>`).

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListEnumerate(list_column) FROM my_table;
```
{% endif %}

## ListReverse {#listreverse}

Развернуть список.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListReverse(list_column) FROM my_table;
```
{% endif %}

## ListSkip {#listskip}

Возвращает копию списка с пропущенным указанным числом первых элементов.

Первый аргумент — исходный список, второй — сколько элементов пропустить.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListSkip(list_column, 3)
FROM my_table;
```
{% endif %}

## ListTake {#listtake}

Возвращает копию списка, состоящую из ограниченного числа элементов второго списка.

Первый аргумент — исходный список, второй — не больше скольких элементов с начала оставить.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListTake(list_column, 3) FROM my_table;
```
{% endif %}

## ListIndexOf {#listindexof}

Ищет элемент с указанным значением в списке и при первом обнаружении возвращает его индекс. Отсчет индексов начинается с 0, а в случае отсутствия элемента возвращается `NULL`.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListIndexOf(list_column, 123)
FROM my_table;
```
{% endif %}

## ListMap, ListFlatMap и ListFilter {#listmap}

Применяют к каждому элементу списка указанную в качестве второго аргумента функцию. Различаются возвращаемым результатом:

* `ListMap` — возвращает список с результатами;
* `ListFlatMap` — возвращает список с результатами, объединяя и разворачивая первый уровень результатов (списков или опциональных значений) по каждому элементу;
* `ListFilter` — оставляет только те элементы, для которых функция вернула `true`.

{% note info %}

В `ListFlatMap` использование опциональных значений в результатах функции является устаревшим, вместо этого следует использовать комбинацию [`ListNotNull`](#listnotnull) и `ListMap`.

{% endnote %}

Аргументы:

1. Исходный список;
2. Функции для обработки элементов, например:
    * [Лямбда функция](../../syntax/expressions.md#lambda);
    * `Module::Function` - С++ UDF;
{% if feature_udf_noncpp %} 
    * [Python UDF](../../udf/python.md), [JavaScript UDF](../../udf/javascript.md) или любое другое вызываемое значение;

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
{% if feature_column_container_type %}
``` yql
$callable = Python::test(Callable<(Int64)->Bool>, "def test(i): return i % 2");
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf),
    ListFilter(list_column, $callable)
FROM my_table;
```
{% endif %}
{% endif %}

## ListNotNull {#listnotnull}

Применяет трансформацию исходного списка, пропуская пустые опциональные элементы, и усиливает тип элемента до неопционального. Для списка с неопциональными элементами возвращает исходный список без изменений.

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
``` yql
SELECT ListNotNull([1,2]),   -- [1,2]
    ListNotNull([3,null,4]); -- [3,4]
```

## ListFlatten {#listflatten}

Разворачивает список списков в плоский список с сохранением порядка элементов. В качестве элемента списка верхнего уровня поддерживается опциональный список, который интерпретируется как пустой в случае `NULL`.

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
``` yql
SELECT ListFlatten([[1,2],[3,4]]),   -- [1,2,3,4]
    ListFlatten([null,[3,4],[5,6]]); -- [3,4,5,6]
```

## ListUniq {#listuniq}

Возвращает копию списка, в котором оставлен только уникальный набор элементов.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListUniq(list_column)
FROM my_table;
```
{% endif %}

## ListAny и ListAll {#listany}

Для списка булевых значений возвращает `true`, если:

* `ListAny` — хотя бы один элемент равен `true`;
* `ListAll` — все элементы равны `true`.

В противном случае возвращает false.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```
{% endif %}

## ListHas {#listhas}

Содержит ли список указанный элемент.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListHas(list_column, "my_needle")
FROM my_table;
```
{% endif %}

## ListHead, ListLast {#listheadlast}

Возвращают первый и последний элемент списка.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListHead(numeric_list_column) AS head,
    ListLast(numeric_list_column) AS last
FROM my_table;
```
{% endif %}

## ListMin, ListMax, ListSum и ListAvg {#listminy}

Применяет соответствующую агрегатную функцию ко всем элементам списка числовых значений.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListMax(numeric_list_column) AS max,
    ListMin(numeric_list_column) AS min,
    ListSum(numeric_list_column) AS sum,
    ListAvg(numeric_list_column) AS avg
FROM my_table;
```
{% endif %}

## ListFold, ListFold1 {#listfold}

Свёртка списка.

Аргументы:

1. Список
2. Начальное состояние U для ListFold, initLambda(item:T)->U для ListFold1
3. updateLambda(item:T, state:U)->U

Возвращаемый тип:
U для ListFold, опциональный U для ListFold1.

**Примеры**

```yql
$l = [1, 4, 7, 2];
$y = ($x, $y) -> { RETURN $x + $y; };
$z = ($x) -> { RETURN 4 * $x; };

SELECT
    ListFold($l, 6, $y) AS fold,                       -- 20
    ListFold([], 3, $y) AS fold_empty,                 -- 3
    ListFold1($l, $z, $y) AS fold1,                    -- 17
    ListFold1([], $z, $y) AS fold1_empty;              -- Null
```

## ListFoldMap, ListFold1Map {#listfoldmap}

Преобразует каждый элемент i в списке путём вызова handler(i, state).

Аргументы:

1. Список
2. Начальное состояние S для ListFoldMap, initLambda(item:T)->кортеж (U S) для ListFold1Map
3. handler(item:T, state:S)->кортеж (U S)

Возвращаемый тип:
Список элементов U.

**Примеры**

```yql
$l = [1, 4, 7, 2];
$x = ($i, $s) -> { RETURN ($i * $s, $i + $s); };
$t = ($i) -> { RETURN ($i + 1, $i + 2); };

SELECT
    ListFoldMap([], 1, $x),                -- []
    ListFoldMap($l, 1, $x),                -- [1, 8, 42, 26]
    ListFold1Map([], $t, $x),              -- []
    ListFold1Map($l, $t, $x);              -- [2, 12, 49, 28]
```

## ListFromRange {#listfromrange}

Генерация последовательности чисел с указанным шагом. Аналог `xrange` в Python 2, но дополнительно с поддержкой чисел с плавающей точкой.

Аргументы:

1. Начало
2. Конец
3. Шаг (опционально, по умолчанию 1)

Особенности:

* Конец не включительный, т.е. `ListFromRange(1,3) == AsList(1,2)`.
* Тип элементов результатов выбирается как наиболее широкий из типов аргументов, например результатом `ListFromRange(1, 2, 0.5)` получится список `Double`.
* Список является «ленивым», но при неправильном использовании всё равно может привести к потреблению большого объема оперативной памяти.
* Если шаг положительный и конец меньше или равен началу, то список будет пустой.
* Если шаг отрицательный и конец больше или равен началу, то список будет пустой.
* Если шаг не положительный и не отрицательный (0 или NaN), то список будет пустой.

**Примеры**
``` yql
SELECT
    ListFromRange(-2, 2), -- [-2, -1, 0, 1]
    ListFromRange(2, 1, -0.5); -- [2.0, 1.5]
```

## ListReplicate {#listreplicate}

Создает список из нескольких копий указанного значения.

Обязательные аргументы:

1. Значение;
2. Число копий.

**Примеры**
``` yql
SELECT ListReplicate(true, 3); -- [true, true, true]
```

## ListConcat {#listconcat}

Объединяет список строк в одну строку.
Вторым параметром можно задать разделитель.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListConcat(string_list_column),
    ListConcat(string_list_column, "; ")
FROM my_table;
```
{% endif %}

## ListExtract {#listextract}

По списку структур возвращает список содержащихся в них полей с указанным именем.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListExtract(struct_list_column, "MyMember")
FROM my_table;
```
{% endif %}

## ListTakeWhile, ListSkipWhile {#listtakewhile}

`ListTakeWhile` выдает список от начала, пока предикат истинный, далее список заканчивается.

`ListSkipWhile` пропускает отрезок списка от начала, пока предикат истинный, далее выдает остаток список не обращая внимания на предикат.
`ListTakeWhileInclusive` выдает список от начала, пока предикат истинный, далее список заканчивается, но также включает элемент, на котором сработал останавливающий предикат.
`ListSkipWhileInclusive` пропускает отрезок списка от начала, пока предикат истинный, далее выдает остаток список не обращая внимания на предикат, но не включая элемент, на котором сработал предикат, а начинает со следующего за ним.

Обязательные аргументы:

1. Список;
2. Предикат.

Если входной список является опциональным, то таким же является и результат.

**Примеры**
``` yql
$data = AsList(1, 2, 5, 1, 2, 7);

SELECT
    ListTakeWhile($data, ($x) -> {return $x <= 3}), -- [1, 2]
    ListSkipWhile($data, ($x) -> {return $x <= 3}), -- [5, 1, 2, 7]
    ListTakeWhileInclusive($data, ($x) -> {return $x <= 3}), -- [1, 2, 5]
    ListSkipWhileInclusive($data, ($x) -> {return $x <= 3}); -- [1, 2, 7]
```

## ListAggregate {#listaggregate}

Применить [фабрику агрегационных функций](../basic.md#aggregationfactory) для переданного списка.
Если переданный список является пустым, то результат агрегации будет такой же, как для пустой таблицы: 0 для функции `COUNT` и `NULL` для других функций.
Если переданный список является опциональным и равен `NULL`, то в результате также будет `NULL`.

Аргументы:

1. Список;
2. [Фабрика агрегационных функций](../basic.md#aggregationfactory).


**Примеры**
``` yql
SELECT ListAggregate(AsList(1, 2, 3), AggregationFactory("Sum")); -- 6
```

## ToDict и ToMultiDict {#todict}

Преобразуют список из кортежей с парами ключ-значение в словарь. В случае конфликтов по ключам во входном списке `ToDict` оставляет первое значение, а `ToMultiDict` — собирает из всех значений список.

Таким образом:

* `ToDict` из `List<Tuple<K, V>>` делает `Dict<K, V>`
* `ToMultiDict` из `List<Tuple<K, V>>` делает `Dict<K, List<V>>`

Также поддерживаются опциональные списки, что приводит к опциональному словарю в результате.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ToDict(tuple_list_column)
FROM my_table;
```
{% endif %}

## ToSet {#toset}
Преобразует список в словарь, в котором ключи являются уникальными элементами этого списка, а значения отсутствуют и имеют тип `Void`. Для списка `List<T>` тип результата будет `Dict<T, Void>`.
Также поддерживается опциональный список, что приводит к опциональному словарю в результате.

Обратная функция - получить список ключей словаря [DictKeys](../dict.md#dictkeys).

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ToSet(list_column)
FROM my_table;
```
{% endif %}
