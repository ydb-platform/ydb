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

**Сигнатура**
```
ListCreate(T)->List<T>
```

## AsList и AsListStrict {#aslist}

Сконструировать список из одного или более аргументов. Типы аргументов должны быть совместимы в случае `AsList` и строго совпадать в случае `AsListStrict`.

**Примеры**
``` yql
SELECT AsList(1, 2, 3, 4, 5);
```

**Сигнатура**
```
AsList(T..)->List<T>
```

## ListLength {#listlength}

Количество элементов в списке.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT ListLength(list_column) FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListLength(List<T>)->Uint64
ListLength(List<T>?)->Uint64?
```

## ListHasItems

Проверка того, что список содержит хотя бы один элемент.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT ListHasItems(list_column) FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListHasItems(List<T>)->Bool
ListHasItems(List<T>?)->Bool?
```

## ListCollect {#listcollect}

Преобразовать ленивый список (строится, например, функциями [ListFilter](#listmap), [ListMap](#listmap), [ListFlatMap](#listmap)) в энергичный. В отличие от ленивого списка, в котором каждый повторный проход заново вычисляет его содержимое, в энергичном списке содержимое списка строится сразу ценой большего потребления памяти.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT ListCollect(list_column) FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListCollect(LazyList<T>)->List<T>
ListCollect(LazyList<T>?)->List<T>?
```

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

**Сигнатура**
```
ListSort(List<T>)->List<T>
ListSort(List<T>?)->List<T>?

ListSort(List<T>, (T)->U)->List<T>
ListSort(List<T>?, (T)->U)->List<T>?
```

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

```yql
$l1 = AsList("a", "b");
$l2 = AsList("b", "c");
$l3 = AsList("d", "e");

SELECT ListExtend($l1, $l2, $l3);  -- ["a","b","b","c","d","e"]
```

**Сигнатура**
```
ListExtend(List<T>..)->List<T>
ListExtend(List<T>?..)->List<T>?
```

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

```yql
$l1 = AsList(
    <|value:1|>,
    <|value:2|>
);
$l2 = AsList(
    <|key:"a"|>,
    <|key:"b"|>
);
SELECT ListUnionAll($l1, $l2);  -- result: [("value":1),("value":2),("key":"a"),("key":"b")]
                                -- schema: List<Struct<key : String?, value : Int32?>>
```

**Сигнатура**
```
ListUnionAll(List<Struct<..>>, List<Struct<..>>..)->List<Struct<..>>
ListUnionAll(List<Struct<..>>?, List<Struct<..>>?..)->List<Struct<..>>?
```

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

```yql
$l1 = AsList("a", "b");
$l2 = AsList(1, 2, 3);

SELECT ListZip($l1, $l2);  -- [("a",1),("b",2)]
SELECT ListZipAll($l1, $l2);  -- [("a",1),("b",2),(null,3)]
```

**Сигнатура**
```
ListZip(List<T1>, List<T2>)->List<Tuple<T1, T2>>
ListZip(List<T1>?, List<T2>?)->List<Tuple<T1, T2>>?

ListZipAll(List<T1>, List<T2>)->List<Tuple<T1?, T2?>>
ListZipAll(List<T1>?, List<T2>?)->List<Tuple<T1?, T2?>>?
```

## ListEnumerate {#listenumerate}

Построить список пар (Tuple), содержащих номер элемента и сам элемент (`List<Tuple<Uint64,list_element_type>>`).

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT ListEnumerate(list_column) FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListEnumerate(List<T>)->List<Tuple<Uint64, T>>
ListEnumerate(List<T>?)->List<Tuple<Uint64, T>>?
```

## ListReverse {#listreverse}

Развернуть список.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT ListReverse(list_column) FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListReverse(List<T>)->List<T>
ListReverse(List<T>?)->List<T>?
```

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

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListSkip($l1, 2);  -- [3,4,5]
```

**Сигнатура**
```
ListSkip(List<T>, Uint64)->List<T>
ListSkip(List<T>?, Uint64)->List<T>?
```

## ListTake {#listtake}

Возвращает копию списка, состоящую из ограниченного числа элементов второго списка.

Первый аргумент — исходный список, второй — не больше скольких элементов с начала оставить.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT ListTake(list_column, 3) FROM my_table;
```
{% endif %}

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListTake($l1, 2);  -- [1,2]
```

**Сигнатура**
```
ListTake(List<T>, Uint64)->List<T>
ListTake(List<T>?, Uint64)->List<T>?
```

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

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListIndexOf($l1, 2);  -- 1
```

**Сигнатура**
```
ListIndexOf(List<T>, T)->Uint64?
ListIndexOf(List<T>?, T)->Uint64?
```

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
{% if feature_udf_noncpp and oss != true %}
    * [Python UDF](../../udf/python.md), [JavaScript UDF](../../udf/javascript.md) или любое другое вызываемое значение;
{% endif %}

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
{% if feature_column_container_type %}
``` yql
SELECT
    ListMap(list_column, ($x) -> { RETURN $x > 2; }),
    ListFlatMap(list_column, My::Udf)
FROM my_table;
```
{% endif %}

```yql
$list = AsList("a", "b", "c");

$filter = ($x) -> {
    RETURN $x == "b";
};

SELECT ListFilter($list, $filter);  -- ["b"]
```

```yql
$list = AsList(1,2,3,4);
$callable = Python::test(Callable<(Int64)->Bool>, "def test(i): return i % 2");
SELECT ListFilter($list, $callable);  -- [1,3]
```

**Сигнатура**
```
ListMap(List<T>, (T)->U)->List<U>
ListMap(List<T>?, (T)->U)->List<U>?

ListFlatMap(List<T>, (T)->List<U>)->List<U>
ListFlatMap(List<T>?, (T)->List<U>)->List<U>?
ListFlatMap(List<T>, (T)->U?)->List<U>
ListFlatMap(List<T>?, (T)->U?)->List<U>?

ListFilter(List<T>, (T)->Bool)->List<T>
ListFilter(List<T>?, (T)->Bool)->List<T>?
```

## ListNotNull {#listnotnull}

Применяет трансформацию исходного списка, пропуская пустые опциональные элементы, и усиливает тип элемента до неопционального. Для списка с неопциональными элементами возвращает исходный список без изменений.

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
``` yql
SELECT ListNotNull([1,2]),   -- [1,2]
    ListNotNull([3,null,4]); -- [3,4]
```

**Сигнатура**
```
ListNotNull(List<T?>)->List<T>
ListNotNull(List<T?>?)->List<T>?
```

## ListFlatten {#listflatten}

Разворачивает список списков в плоский список с сохранением порядка элементов. В качестве элемента списка верхнего уровня поддерживается опциональный список, который интерпретируется как пустой в случае `NULL`.

Если исходный список является опциональным, то таким же является и выходной список.

**Примеры**
``` yql
SELECT ListFlatten([[1,2],[3,4]]),   -- [1,2,3,4]
    ListFlatten([null,[3,4],[5,6]]); -- [3,4,5,6]
```

**Сигнатура**
```
ListFlatten(List<List<T>?>)->List<T>
ListFlatten(List<List<T>?>?)->List<T>?
```

## ListUniq и ListUniqStable {#listuniq}

Возвращает копию списка, в котором оставлен только уникальный набор элементов. В случае ListUniq порядок элементов результирующего набора не определен, в случае ListUniqStable элементы находятся в порядке вхождения в исходный список.

**Примеры**
``` yql
SELECT ListUniq([1, 2, 3, 2, 4, 5, 1]) -- [5, 4, 2, 1, 3]
SELECT ListUniqStable([1, 2, 3, 2, 4, 5, 1]) -- [1, 2, 3, 4, 5]
SELECT ListUniqStable([1, 2, null, 7, 2, 8, null]) -- [1, 2, null, 7, 8]
```

**Сигнатура**
```
ListUniq(List<T>)->List<T>
ListUniq(List<T>?)->List<T>?

ListUniqStable(List<T>)->List<T>
ListUniqStable(List<T>?)->List<T>?
```

## ListAny и ListAll {#listany}

Для списка булевых значений возвращает `true`, если:

* `ListAny` — хотя бы один элемент равен `true`;
* `ListAll` — все элементы равны `true`.

В противном случае возвращает false.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT
    ListAll(bool_column),
    ListAny(bool_column)
FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListAny(List<Bool>)->Bool
ListAny(List<Bool>?)->Bool?
ListAll(List<Bool>)->Bool
ListAll(List<Bool>?)->Bool?
```

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

```yql
$l1 = AsList(1, 2, 3, 4, 5);

SELECT ListHas($l1, 2);  -- true
SELECT ListHas($l1, 6);  -- false
```

**Сигнатура**
```
ListHas(List<T>, T)->Bool
ListHas(List<T>?, T)->Bool?
```

## ListHead, ListLast {#listheadlast}

Возвращают первый и последний элемент списка.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT
    ListHead(numeric_list_column) AS head,
    ListLast(numeric_list_column) AS last
FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListHead(List<T>)->T?
ListHead(List<T>?)->T?
ListLast(List<T>)->T?
ListLast(List<T>?)->T?
```

## ListMin, ListMax, ListSum и ListAvg {#listminy}

Применяет соответствующую агрегатную функцию ко всем элементам списка числовых значений.

{% if feature_column_container_type %}
**Примеры**
``` yql
SELECT
    ListMax(numeric_list_column) AS max,
    ListMin(numeric_list_column) AS min,
    ListSum(numeric_list_column) AS sum,
    ListAvg(numeric_list_column) AS avg
FROM my_table;
```
{% endif %}

**Сигнатура**
```
ListMin(List<T>)->T?
ListMin(List<T>?)->T?
```

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

**Сигнатура**
```
ListFold(List<T>, U, (T, U)->U)->U
ListFold(List<T>?, U, (T, U)->U)->U?

ListFold1(List<T>, (T)->U, (T, U)->U)->U?
ListFold1(List<T>?, (T)->U, (T, U)->U)->U?
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

**Сигнатура**
```
ListFoldMap(List<T>, S, (T, S)->Tuple<U,S>)->List<U>
ListFoldMap(List<T>?, S, (T, S)->Tuple<U,S>)->List<U>?

ListFold1Map(List<T>, (T)->Tuple<U,S>, (T, S)->Tuple<U,S>)->List<U>
ListFold1Map(List<T>?, (T)->Tuple<U,S>, (T, S)->Tuple<U,S>)->List<U>?
```

## ListFromRange {#listfromrange}

Генерация последовательности чисел или дат с указанным шагом. Аналог `xrange` в Python 2, но дополнительно с поддержкой дат и чисел с плавающей точкой.

Аргументы:

1. Начало
2. Конец
3. Шаг. Опционально, по умолчанию 1 для числовых последовательностей, 1 день для `Date`/`TzDate`, 1 секунда для `Datetime`/`TzDatetime` и 1 микросекунда для `Timestamp`/`TzTimestamp`/`Interval`

Особенности:

* Конец не включительный, т.е. `ListFromRange(1,3) == AsList(1,2)`.
* Тип элементов результатов выбирается как наиболее широкий из типов аргументов, например результатом `ListFromRange(1, 2, 0.5)` получится список `Double`.
* Если начало и конец имеют один из типов дат, то шаг должен иметь тип `Interval`.
* Список является «ленивым», но при неправильном использовании всё равно может привести к потреблению большого объема оперативной памяти.
* Если шаг положительный и конец меньше или равен началу, то список будет пустой.
* Если шаг отрицательный и конец больше или равен началу, то список будет пустой.
* Если шаг не положительный и не отрицательный (0 или NaN), то список будет пустой.
* Если один из параметров опциональный, то результат будет опциональный список.
* Если один из параметров равен `NULL`, то результат будет `NULL`.

**Примеры**
``` yql
SELECT
    ListFromRange(-2, 2), -- [-2, -1, 0, 1]
    ListFromRange(2, 1, -0.5); -- [2.0, 1.5]
```

``` yql
SELECT ListFromRange(Datetime("2022-05-23T15:30:00Z"), Datetime("2022-05-30T15:30:00Z"), DateTime::IntervalFromDays(1));
```

**Сигнатура**
```
ListFromRange(T, T)->LazyList<T> -- T - числовой тип или тип, представляющий дату/время
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

**Сигнатура**
```
ListReplicate(T, Uint64)->List<T>
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

```yql
$l1 = AsList("h", "e", "l", "l", "o");

SELECT ListConcat($l1);  -- "hello"
SELECT ListConcat($l1, " ");  -- "h e l l o"
```

**Сигнатура**
```
ListConcat(List<String>)->String?
ListConcat(List<String>?)->String?

ListConcat(List<String>, String)->String?
ListConcat(List<String>?, String)->String?
```

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

```sql
$l = AsList(
    <|key:"a", value:1|>,
    <|key:"b", value:2|>
);
SELECT ListExtract($l, "key");  -- ["a", "b"]
```

**Сигнатура**
```
ListExtract(List<Struct<..>>, String)->List<T>
ListExtract(List<Struct<..>>?, String)->List<T>?
```

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

**Сигнатура**
```
ListTakeWhile(List<T>, (T)->Bool)->List<T>
ListTakeWhile(List<T>?, (T)->Bool)->List<T>?
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

**Сигнатура**
```
ListAggregate(List<T>, AggregationFactory)->T
ListAggregate(List<T>?, AggregationFactory)->T?
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

```yql
$l = AsList(("a",1), ("b", 2), ("a", 3));
SELECT ToDict($l);  -- {"a": 1,"b": 2}
```

**Сигнатура**
```
ToDict(List<Tuple<K,V>>)->Dict<K,V>
ToDict(List<Tuple<K,V>>?)->Dict<K,V>?
```

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

```yql
$l = AsList(1,1,2,2,3);
SELECT ToSet($l);  -- {1,2,3}
```

**Сигнатура**
```
ToSet(List<T>)->Set<T>
ToSet(List<T>?)->Set<T>?
```

## ListFromTuple
Строит список из кортежа, в котором типы элементов совместимы друг с другом. Для опционального кортежа на выходе получается опциональный список. Для NULL аргумента - NULL. Для пустого кортежа - EmptyList.

**Примеры**
```yql
$t = (1,2,3);
SELECT ListFromTuple($t);  -- [1,2,3]
```

**Сигнатура**
```
ListFromTuple(Null)->Null
ListFromTuple(Tuple<>)->EmptyList
ListFromTuple(Tuple<T1,T2,...>)->List<T>
ListFromTuple(Tuple<T1,T2,...>?)->List<T>?
```

## ListToTuple
Строит кортеж из списка и явно указанной ширины кортежа. Все элементы кортежа будут иметь тот же тип, что и тип элемента списка. Если длина списка не соотвествует указанной ширине кортежа, будет возвращена ошибка. Для опционального списка на выходе получается опциональный кортеж. Для NULL аргумента - NULL.

**Примеры**
```yql
$l = [1,2,3];
SELECT ListToTuple($l, 3);  -- (1,2,3)
```

**Сигнатура**
```
ListToTuple(Null,N)->Null
ListToTuple(EmptyList,N)->()) -- N должен быть 0
ListToTuple(List<T>, N)->Tuple<T,T,...T> -- ширина кортежа N
ListToTuple(List<T>?, N)->Tuple<T,T,...T>? -- ширина кортежа N
```

## ListTop, ListTopAsc, ListTopDesc, ListTopSort, ListTopSortAsc и ListTopSortDesc {#listtop}

Выбрать топ значениий из списка. `ListTopSort*` — дополнительно отсортировать список возвращенных значений. По умолчанию выбираются наименьшие значения (функции без суффикса — алиас к функциям `*Asc`; `*Desc` — выбор наибольших значений).

`ListTopSort` эффективнее, чем последовательные `ListTop` и `ListSort`, так как `ListTop` может уже частично отсортировать список для поиска нужных значений. Однако, `ListTop` эффективнее `ListTopSort`,  если сортировка не нужна.

Аргументы:

1. Список;
2. Размер выборки;
3. Опциональное выражение для получения ключа сортировки из элемента списка (по умолчанию сам элемент).

**Сигнатура**
```
ListTop(List<T>{Flags:AutoMap}, N)->List<T>
ListTop(List<T>{Flags:AutoMap}, N, (T)->U)->List<T>
```
Сигнатуры остальных функций совпадают с `ListTop`.
