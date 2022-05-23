# Функции для работы со словарями

## DictCreate {#dictcreate}
**Сигнатура**
```
DictCreate(K,V)->Dict<K,V>
```

Сконструировать пустой словарь. Передается два аргумента — для ключа и значения, в каждом из которых указывается строка с описанием типа данных, либо сам тип, полученный с помощью [предназначенных для этого функций](../types.md). Словарей с неизвестным типом ключа или значения в YQL не бывает.
В качестве типа ключа могут быть заданы:
* [примитивный тип данных](../../types/primitive.md) (кроме `Yson` и `Json`),
* примитивный тип данных (кроме `Yson` и `Json`) с признаком опциональности,
* кортеж длины не менее два из типов, перечисленных выше.

[Документация по формату описания типа](../../types/type_string.md).

**Примеры**
``` yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

``` yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

``` yql
SELECT DictCreate(ParseType("Tuple<Int32?,String>"), ParseType("Tuple<String,Double?>"));
```

## SetCreate {#setcreate}
**Сигнатура**
```
SetCreate(T)->Set<T>
```

Сконструировать пустое множество. Передается аргумент - тип ключа, возможно, полученный с помощью [предназначенных для этого функций](../types.md). Множеств с неизвестным типом ключа в YQL не бывает. Ограничения на тип ключа такие же как и на тип ключа для словаря. Следует иметь ввиду, что множество это словарь с типом значения `Void` и множество также можно создать и с помощью функции `DictCreate`. Отсюда также следует, что все функции, которые принимают на вход `Dict<K,V>` могут также принимать `Set<K>`.

[Документация по формату описания типа](../../types/type_string.md).

**Примеры**
``` yql
SELECT SetCreate(String);
```

``` yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}
**Сигнатура**
```
DictLength(Dict<K,V>)->Uint64
DictLength(Dict<K,V>?)->Uint64?
```

Количество элементов в словаре.

**Примеры**
``` yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```
{% if feature_column_container_type %}
``` yql
SELECT DictLength(dict_column) FROM my_table;
```
{% endif %}
## DictHasItems {#dicthasitems}
**Сигнатура**
```
DictHasItems(Dict<K,V>)->Bool
DictHasItems(Dict<K,V>?)->Bool?
```

Проверка того, что словарь содержит хотя бы один элемент.

**Примеры**
``` yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```
{% if feature_column_container_type %}
``` yql
SELECT DictHasItems(dict_column) FROM my_table;
```
{% endif %}


## DictItems {#dictitems}
**Сигнатура**
```
DictItems(Dict<K,V>)->List<Tuple<K,V>>
DictItems(Dict<K,V>?)->List<Tuple<K,V>>?
```

Получение содержимого словаря в виде списка кортежей с парами ключ-значение (`List<Tuple<key_type,value_type>>`).

**Примеры**

``` yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```
{% if feature_column_container_type %}
``` yql
SELECT DictItems(dict_column)
FROM my_table;
```
{% endif %}
## DictKeys {#dictkeys}
**Сигнатура**
```
DictKeys(Dict<K,V>)->List<K>
DictKeys(Dict<K,V>?)->List<K>?
```

Получение списка ключей словаря.

**Примеры**

``` yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```
{% if feature_column_container_type %}
``` yql
SELECT DictKeys(dict_column)
FROM my_table;
```
{% endif %}
## DictPayloads {#dictpayloads}
**Сигнатура**
```
DictPayloads(Dict<K,V>)->List<V>
DictPayloads(Dict<K,V>?)->List<V>?
```

Получение списка значений словаря.

**Примеры**

``` yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```
{% if feature_column_container_type %}
``` yql
SELECT DictPayloads(dict_column)
FROM my_table;
```
{% endif %}

## DictLookup {#dictlookup}
**Сигнатура**
```
DictLookup(Dict<K,V>, K)->V?
DictLookup(Dict<K,V>?, K)->V?
DictLookup(Dict<K,V>, K?)->V?
DictLookup(Dict<K,V>?, K?)->V?
```

Получение элемента словаря по ключу.

**Примеры**

``` yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```
{% if feature_column_container_type %}
``` yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```
{% endif %}

## DictContains {#dictcontains}
**Сигнатура**
```
DictContains(Dict<K,V>, K)->Bool
DictContains(Dict<K,V>?, K)->Bool
DictContains(Dict<K,V>, K?)->Bool
DictContains(Dict<K,V>?, K?)->Bool
```

Проверка наличия элемента в словаре по ключу. Возвращает true или false.

**Примеры**

``` yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```
{% if feature_column_container_type %}
``` yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```
{% endif %}

## DictAggregate {#dictaggregate}
**Сигнатура**
```
DictAggregate(Dict<K,List<V>>, List<V>->T)->Dict<K,T>
DictAggregate(Dict<K,List<V>>?, List<V>->T)->Dict<K,T>?
```

Применить [фабрику агрегационных функций](../basic.md#aggregationfactory) для переданного словаря, в котором каждое значение является списком. Фабрика применяется отдельно внутри каждого ключа.
Если список является пустым, то результат агрегации будет такой же, как для пустой таблицы: 0 для функции `COUNT` и `NULL` для других функций.
Если в переданном словаре список по некоторому ключу является пустым, то такой ключ удаляется из результата.
Если переданный словарь является опциональным и содержит значение `NULL`, то в результате также будет `NULL`.

Аргументы:

1. Словарь;
2. [Фабрика агрегационных функций](../basic.md#aggregationfactory).


**Примеры**

```sql
SELECT DictAggregate(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("baz", "qwe"))),
    AggregationFactory("Max"));
-- {1 : "foo", 2 : "qwe" }

```

## SetIsDisjoint {#setisjoint}
**Сигнатура**
```
SetIsDisjoint(Dict<K,V1>, Dict<K,V2>)->Bool
SetIsDisjoint(Dict<K,V1>?, Dict<K,V2>)->Bool?
SetIsDisjoint(Dict<K,V1>, Dict<K,V2>?)->Bool?
SetIsDisjoint(Dict<K,V1>?, Dict<K,V2>?)->Bool?

SetIsDisjoint(Dict<K,V1>, List<K>)->Bool
SetIsDisjoint(Dict<K,V1>?, List<K>)->Bool?
SetIsDisjoint(Dict<K,V1>, List<K>?)->Bool?
SetIsDisjoint(Dict<K,V1>?, List<K>?)->Bool?
```

Проверка того, что словарь и список или другой словарь не пересекаются по ключам.

Таким образом есть два варианта вызова:

* С аргументами `Dict<K,V1>` и `List<K>`;
* С аргументами `Dict<K,V1>` и `Dict<K,V2>`.

**Примеры**

```sql
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4)); -- true
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- false
```

## SetIntersection {#setintersection}
**Сигнатура**
```
SetIntersection(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetIntersection(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetIntersection(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetIntersection(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetIntersection(Dict<K,V1>, Dict<K,V2>, (K,V1,V2)->U)->Dict<K,U>
SetIntersection(Dict<K,V1>?, Dict<K,V2>, (K,V1,V2)->U)->Dict<K,U>?
SetIntersection(Dict<K,V1>, Dict<K,V2>?, (K,V1,V2)->U)->Dict<K,U>?
SetIntersection(Dict<K,V1>?, Dict<K,V2>?, (K,V1,V2)->U)->Dict<K,U>?
```

Строит пересечение двух словарей по ключам.

Аргументы:

* Два словаря: `Dict<K,V1>` и `Dict<K,V2>`.
* Необязательная функция, которая объединяет значения из исходных словарей для построения значений выходного словаря. Если тип такой функции `(K,V1,V2) -> U`, то типом результата будет `Dict<K,U>`. Если функция не задана, типом результата будет `Dict<K,Void>`, а значения из исходных словарей игнорируются.

**Примеры**
``` yql
SELECT SetIntersection(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 3 }
SELECT SetIntersection(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz") }
```

{% note info %}

В примере использовалась [лямбда функция](../../syntax/expressions.md#lambda).

{% endnote %}

## SetIncludes {#setincludes}
**Сигнатура**
```
SetIncludes(Dict<K,V1>, List<K>)->Bool
SetIncludes(Dict<K,V1>?, List<K>)->Bool?
SetIncludes(Dict<K,V1>, List<K>?)->Bool?
SetIncludes(Dict<K,V1>?, List<K>?)->Bool?

SetIncludes(Dict<K,V1>, Dict<K,V2>)->Bool
SetIncludes(Dict<K,V1>?, Dict<K,V2>)->Bool?
SetIncludes(Dict<K,V1>, Dict<K,V2>?)->Bool?
SetIncludes(Dict<K,V1>?, Dict<K,V2>?)->Bool?
```

Проверка того, что в ключи заданного словаря входят все элементы списка или ключи второго словаря.

Таким образом есть два варианта вызова:

* С аргументами `Dict<K,V1>` и `List<K>`;
* С аргументами `Dict<K,V1>` и `Dict<K,V2>`.

**Примеры**
``` yql
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4)); -- false
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))); -- true
```

## SetUnion {#setunion}
**Сигнатура**
```
SetUnion(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetUnion(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetUnion(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetUnion(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetUnion(Dict<K,V1>, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>
SetUnion(Dict<K,V1>?, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>?
SetUnion(Dict<K,V1>, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
SetUnion(Dict<K,V1>?, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
```

Строит объединение двух словарей по ключам.

Аргументы:

* Два словаря: `Dict<K,V1>` и `Dict<K,V2>`.
* Необязательная функция, которая объединяет значения из исходных словарей для построения значений выходного словаря. Если тип такой функции `(K,V1?,V2?) -> U`, то типом результата будет `Dict<K,U>`. Если функция не задана, типом результата будет `Dict<K,Void>`, а значения из исходных словарей игнорируются.

**Примеры**
``` yql
SELECT SetUnion(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 3, 4 }
SELECT SetUnion(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz"), 2 : (null, "qwe"), 3 : ("bar", null) }
```

## SetDifference {#setdifference}
**Сигнатура**
```
SetDifference(Dict<K,V1>, Dict<K,V2>)->Dict<K,V1>
SetDifference(Dict<K,V1>?, Dict<K,V2>)->Dict<K,V1>?
SetDifference(Dict<K,V1>, Dict<K,V2>?)->Dict<K,V1>?
SetDifference(Dict<K,V1>?, Dict<K,V2>?)->Dict<K,V1>?
```

Строит словарь, в котором есть все ключи с соответствующими значениями первого словаря, для которых нет ключа во втором словаре.

**Примеры**
``` yql
SELECT SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2 }
SELECT SetDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(2, "bar")),
    ToSet(AsList(2, 3)));
-- { 1 : "foo" }
```

## SetSymmetricDifference {#setsymmetricdifference}
**Сигнатура**
```
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>)->Set<K>
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>)->Set<K>?
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>?)->Set<K>?
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>?)->Set<K>?

SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>,(K,V1?,V2?)->U)->Dict<K,U>?
SetSymmetricDifference(Dict<K,V1>, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
SetSymmetricDifference(Dict<K,V1>?, Dict<K,V2>?,(K,V1?,V2?)->U)->Dict<K,U>?
```

Строит симметрическую разность двух словарей по ключам.

Аргументы:

* Два словаря: `Dict<K,V1>` и `Dict<K,V2>`.
* Необязательная функция, которая объединяет значения из исходных словарей для построения значений выходного словаря. Если тип такой функции `(K,V1?,V2?) -> U`, то типом результата будет `Dict<K,U>`. Если функция не задана, типом результата будет `Dict<K,Void>`, а значения из исходных словарей игнорируются.

**Примеры**
``` yql
SELECT SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 4 }
SELECT SetSymmetricDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 2 : (null, "qwe"), 3 : ("bar", null) }
```
