# Функции для работы со словарями

## DictCreate {#dictcreate}

Сконструировать пустой словарь. Передается два аргумента — для ключа и значения, в каждом из которых указывается строка с описанием типа данных, либо сам тип, полученный с помощью [предназначенных для этого функций](../types.md). Словарей с неизвестным типом ключа или значения в YQL не бывает. В качестве ключа может быть задан [примитивный тип данных](../../types/primitive.md) за исключением `Yson` и `Json` с необязательным признаком [опциональности](../../types/optional.md), или кортеж из них длины не менее два.

[Документация по формату описания типа](../../types/type_string.md).

**Примеры**
``` yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

``` yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

## SetCreate {#setcreate}

Сконструировать пустое множество. Передается аргумент - тип ключа, возможно, полученный с помощью [предназначенных для этого функций](../types.md). Множеств с неизвестным типом ключа в YQL не бывает. В качестве ключа может быть задан [примитивный тип данных](../../types/primitive.md) за исключением `Yson` и `Json` с необязательным признаком [опциональности](../../types/optional.md), или кортеж из них длины не менее два.

[Документация по формату описания типа](../../types/type_string.md).

**Примеры**
``` yql
SELECT SetCreate(String);
```

``` yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}

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

## SetIncludes {#setincludes}

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
