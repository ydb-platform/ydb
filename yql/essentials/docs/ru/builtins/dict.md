
# Функции для работы со словарями

## DictCreate {#dictcreate}

#### Сигнатура

```yql
DictCreate(K,V)->Dict<K,V>
```

Сконструировать пустой словарь. Передается два аргумента — для ключа и значения, в каждом из которых указывается строка с описанием типа данных, либо сам тип, полученный с помощью [предназначенных для этого функций](types.md). Словарей с неизвестным типом ключа или значения в YQL не бывает.

В качестве типа ключа могут быть заданы:

* [примитивный тип данных](../types/primitive.md) (кроме `Yson` и `Json`),
* примитивный тип данных (кроме `Yson` и `Json`) с признаком опциональности,
* кортеж длины не менее два из типов, перечисленных выше.

[Документация по формату описания типа](../types/type_string.md).

#### Примеры

```yql
SELECT DictCreate(String, Tuple<String,Double?>);
```

```yql
SELECT DictCreate(Tuple<Int32?,String>, OptionalType(DataType("String")));
```

```yql
SELECT DictCreate(ParseType("Tuple<Int32?,String>"), ParseType("Tuple<String,Double?>"));
```

## SetCreate {#setcreate}

#### Сигнатура

```yql
SetCreate(T)->Set<T>
```

Сконструировать пустое множество. Передается аргумент - тип ключа, возможно, полученный с помощью [предназначенных для этого функций](types.md). Множеств с неизвестным типом ключа в YQL не бывает. Ограничения на тип ключа такие же как и на тип ключа для словаря. Следует иметь ввиду, что множество это словарь с типом значения `Void` и множество также можно создать и с помощью функции `DictCreate`. Отсюда также следует, что все функции, которые принимают на вход `Dict<K,V>` могут также принимать `Set<K>`.

[Документация по формату описания типа](../types/type_string.md).

#### Примеры

```yql
SELECT SetCreate(String);
```

```yql
SELECT SetCreate(Tuple<Int32?,String>);
```

## DictLength {#dictlength}

#### Сигнатура

```yql
DictLength(Dict<K,V>)->Uint64
DictLength(Dict<K,V>?)->Uint64?
```

Количество элементов в словаре.

#### Примеры

```yql
SELECT DictLength(AsDict(AsTuple(1, AsList("foo", "bar"))));
```

```yql
SELECT DictLength(dict_column) FROM my_table;
```

## DictHasItems {#dicthasitems}

#### Сигнатура

```yql
DictHasItems(Dict<K,V>)->Bool
DictHasItems(Dict<K,V>?)->Bool?
```

Проверка того, что словарь содержит хотя бы один элемент.

#### Примеры

```yql
SELECT DictHasItems(AsDict(AsTuple(1, AsList("foo", "bar")))) FROM my_table;
```

```yql
SELECT DictHasItems(dict_column) FROM my_table;
```

## DictItems {#dictitems}

#### Сигнатура

```yql
DictItems(Dict<K,V>)->List<Tuple<K,V>>
DictItems(Dict<K,V>?)->List<Tuple<K,V>>?
```

Получение содержимого словаря в виде списка кортежей с парами ключ-значение (`List<Tuple<key_type,value_type>>`).

#### Примеры

```yql
SELECT DictItems(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ ( 1, [ "foo", "bar" ] ) ]
```

```yql
SELECT DictItems(dict_column)
FROM my_table;
```

## DictKeys {#dictkeys}

#### Сигнатура

```yql
DictKeys(Dict<K,V>)->List<K>
DictKeys(Dict<K,V>?)->List<K>?
```

Получение списка ключей словаря.

#### Примеры

```yql
SELECT DictKeys(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ 1 ]
```

```yql
SELECT DictKeys(dict_column)
FROM my_table;
```

## DictPayloads {#dictpayloads}

#### Сигнатура

```yql
DictPayloads(Dict<K,V>)->List<V>
DictPayloads(Dict<K,V>?)->List<V>?
```

Получение списка значений словаря.

#### Примеры

```yql
SELECT DictPayloads(AsDict(AsTuple(1, AsList("foo", "bar"))));
-- [ [ "foo", "bar" ] ]
```

```yql
SELECT DictPayloads(dict_column)
FROM my_table;
```

## DictLookup {#dictlookup}

#### Сигнатура

```yql
DictLookup(Dict<K,V>, K)->V?
DictLookup(Dict<K,V>?, K)->V?
DictLookup(Dict<K,V>, K?)->V?
DictLookup(Dict<K,V>?, K?)->V?
```

Получение элемента словаря по ключу.

#### Примеры

```yql
SELECT DictLookup(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 1);
-- [ "foo", "bar" ]
```

```yql
SELECT DictLookup(dict_column, "foo")
FROM my_table;
```

## DictContains {#dictcontains}

#### Сигнатура

```yql
DictContains(Dict<K,V>, K)->Bool
DictContains(Dict<K,V>?, K)->Bool
DictContains(Dict<K,V>, K?)->Bool
DictContains(Dict<K,V>?, K?)->Bool
```

Проверка наличия элемента в словаре по ключу. Возвращает true или false.

#### Примеры

```yql
SELECT DictContains(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("bar", "baz"))
), 42);
-- false
```

```yql
SELECT DictContains(dict_column, "foo")
FROM my_table;
```

## DictAggregate {#dictaggregate}

#### Сигнатура

```yql
DictAggregate(Dict<K,List<V>>, List<V>->T)->Dict<K,T>
DictAggregate(Dict<K,List<V>>?, List<V>->T)->Dict<K,T>?
```

Применить [фабрику агрегационных функций](basic.md#aggregationfactory) для переданного словаря, в котором каждое значение является списком. Фабрика применяется отдельно внутри каждого ключа.
Если список является пустым, то результат агрегации будет такой же, как для пустой таблицы: 0 для функции `COUNT` и `NULL` для других функций.
Если в переданном словаре список по некоторому ключу является пустым, то такой ключ удаляется из результата.
Если переданный словарь является опциональным и содержит значение `NULL`, то в результате также будет `NULL`.

Аргументы:

1. Словарь;
2. [Фабрика агрегационных функций](basic.md#aggregationfactory).


#### Примеры

```yql
SELECT DictAggregate(AsDict(
    AsTuple(1, AsList("foo", "bar")),
    AsTuple(2, AsList("baz", "qwe"))),
    AggregationFactory("Max"));
-- {1 : "foo", 2 : "qwe" }
```

## SetIsDisjoint {#setisjoint}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), AsList(7, 4)); -- true
SELECT SetIsDisjoint(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- false
```

## SetIntersection {#setintersection}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT SetIntersection(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 3 }
SELECT SetIntersection(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz") }
```

{% note info %}

В примере использовалась [лямбда функция](../syntax/expressions.md#lambda).

{% endnote %}

## SetIncludes {#setincludes}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), AsList(3, 4)); -- false
SELECT SetIncludes(ToSet(AsList(1, 2, 3)), ToSet(AsList(2, 3))); -- true
```

## SetUnion {#setunion}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT SetUnion(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 3, 4 }
SELECT SetUnion(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 1 : ("foo", "baz"), 2 : (null, "qwe"), 3 : ("bar", null) }
```

## SetDifference {#setdifference}

#### Сигнатура

```yql
SetDifference(Dict<K,V1>, Dict<K,V2>)->Dict<K,V1>
SetDifference(Dict<K,V1>?, Dict<K,V2>)->Dict<K,V1>?
SetDifference(Dict<K,V1>, Dict<K,V2>?)->Dict<K,V1>?
SetDifference(Dict<K,V1>?, Dict<K,V2>?)->Dict<K,V1>?
```

Строит словарь, в котором есть все ключи с соответствующими значениями первого словаря, для которых нет ключа во втором словаре.

#### Примеры

```yql
SELECT SetDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2 }
SELECT SetDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(2, "bar")),
    ToSet(AsList(2, 3)));
-- { 1 : "foo" }
```

## SetSymmetricDifference {#setsymmetricdifference}

#### Сигнатура

```yql
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

#### Примеры

```yql
SELECT SetSymmetricDifference(ToSet(AsList(1, 2, 3)), ToSet(AsList(3, 4))); -- { 1, 2, 4 }
SELECT SetSymmetricDifference(
    AsDict(AsTuple(1, "foo"), AsTuple(3, "bar")),
    AsDict(AsTuple(1, "baz"), AsTuple(2, "qwe")),
    ($k, $a, $b) -> { RETURN AsTuple($a, $b) });
-- { 2 : (null, "qwe"), 3 : ("bar", null) }
```

## DictInsert {#dictinsert}

#### Сигнатура

```yql
DictInsert(Dict<K,V>,K,V)->Dict<K,V>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Возвращает новый словарь в который добавили заданный ключ и значение. Если ключ уже существовал, словарь не изменяется.
При работе с множеством (`Set`) в качестве значения следует передавать функцию `Void()`.

#### Примеры

```yql
SELECT DictInsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictInsert({'foo':1}, 'foo', 2); -- {'foo':1}
SELECT DictInsert({'foo'}, 'bar', Void()); -- {'foo','bar'}
```

## DictUpsert {#dictupsert}

#### Сигнатура

```yql
DictUpsert(Dict<K,V>,K,V)->Dict<K,V>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Возвращает новый словарь в который добавили или заменили заданный ключ и значение. Если ключ уже существовал, значение обновляется.

#### Примеры

```yql
SELECT DictUpsert({'foo':1}, 'bar', 2); -- {'foo':1,'bar':2}
SELECT DictUpsert({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictUpdate {#dictupdate}

#### Сигнатура

```yql
DictUpdate(Dict<K,V>,K,V)->Dict<K,V>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Возвращает новый словарь в который заменили значение по заданному ключу. Если ключ не существовал, словарь не меняется.

#### Примеры

```yql
SELECT DictUpdate({'foo':1}, 'bar', 2); -- {'foo':1}
SELECT DictUpdate({'foo':1}, 'foo', 2); -- {'foo':2}
```

## DictRemove {#dictremove}

#### Сигнатура

```yql
DictRemove(Dict<K,V>,K)->Dict<K,V>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Возвращает новый словарь без заданного ключа. Если ключ не существовал, словарь не меняется.

#### Примеры

```yql
SELECT DictRemove({'foo':1}, 'bar'); -- {'foo':1}
SELECT DictRemove({'foo':1}, 'foo'); -- {}
```

## ToMutDict {#tomutdict}

#### Сигнатура

```yql
ToMutDict(Dict<K,V>,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Конвертирует словарь в свою мутабельную версию. Также необходимо передать одно или более зависимое выражение, например, используя аргумент `lambda` в функции [`Block`](basic.md#block).

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictCreate {#mutdictcreate}

#### Сигнатура

```yql
MutDictCreate(KeyType,ValueType,dependArg1...)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Строит пустой мутабельный словарь с заданными типами ключа и значения. Также необходимо передать одно или более зависимое выражение, например, используя аргумент `lambda` в функции [`Block`](basic.md#block).

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        return FromMutDict($dict);
    }); -- {}
```

## FromMutDict {#frommutdict}

#### Сигнатура

```yql
FromMutDict(Linear<mutDictType for Dict<K,V>>)->Dict<K,V>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Поглощает мутабельный словарь и преобразует его в иммутабельный.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictInsert {#mutdictinsert}

#### Сигнатура

```yql
MutDictInsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Добавляет в мутабельный словарь заданные ключ и значение, возвращает этот же мутабельный словарь. Если ключ в словаре уже существовал, словарь не меняется.
При работе с множеством (`Set`) в качестве значения следует передавать функцию `Void()`.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':1}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictInsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo'}, $arg);
        $dict = MutDictInsert($dict,'bar', Void());
        return FromMutDict($dict);
    }); -- {'foo','bar'}
```

## MutDictUpsert {#mutdictupsert}

#### Сигнатура

```yql
MutDictUpsert(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Добавляет или заменяет в мутабельный словаре заданные ключ и значение, возвращает этот же мутабельный словарь. Если ключ в словаре уже существовал, значение обновляется.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpsert($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1,'bar':2}
```

## MutDictUpdate {#mutdictupdate}

#### Сигнатура

```yql
MutDictUpdate(Linear<mutDictType for Dict<K,V>>,K,V)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Заменяет в мутабельном словаре значение по заданному ключу, возвращает этот же мутабельный словарь. Если ключ в словаре не существовал, словарь не меняется.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'foo',2);
        return FromMutDict($dict);
    }); -- {'foo':2}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictUpdate($dict,'bar',2);
        return FromMutDict($dict);
    }); -- {'foo':1}
```

## MutDictRemove {#mutdictremove}

#### Сигнатура

```yql
MutDictRemove(Linear<mutDictType for Dict<K,V>>,K)->Linear<mutDictType for Dict<K,V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Удаляет в мутабельном словаре значение по заданному ключу, возвращает этот же мутабельный словарь. Если ключ в словаре не существовал, словарь не меняется.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'foo');
        return FromMutDict($dict);
    }); -- {}

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict = MutDictRemove($dict,'bar');
        return FromMutDict($dict);
    }); -- {'foo':1}
```


## MutDictPop {#mutdictpop}

#### Сигнатура

```yql
MutDictPop(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Удаляет в мутабельном словаре значение по заданному ключу, возвращает этот же мутабельный словарь и значение по удаленному ключу. Если ключ в словаре не существовал, словарь не меняется и возвращается пустой Optional.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPop($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictContains {#mutdictcontains}

#### Сигнатура

```yql
MutDictContains(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Проверяет существования ключа в мутабельном словаре, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictContains($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},False)
```

## MutDictLookup {#mutdictlookup}

#### Сигнатура

```yql
MutDictLookup(Linear<mutDictType for Dict<K,V>>,K)->Tuple<Linear<mutDictType for Dict<K,V>>,V?>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Получает значение по ключу в мутабельном словаре, возвращает этот же мутабельный словарь и опциональный результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'foo');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLookup($dict,'bar');
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},null)
```

## MutDictHasItems {#mutdicthasitems}

#### Сигнатура

```yql
MutDictHasItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Bool>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Проверяет непустоту мутабельного словаря, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},True)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictHasItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},False)
```

## MutDictLength {#mutdictlength}

#### Сигнатура

```yql
MutDictLength(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,Uint64>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Получает количество элементов в мутабельном словаре, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},1)

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictLength($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},0)
```

## MutDictKeys {#mutdictkeys}

#### Сигнатура

```yql
MutDictKeys(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<K>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Получает список ключей в мутабельном словаре, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['foo'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictKeys($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictPayloads {#mutdictpayloads}

#### Сигнатура

```yql
MutDictPayloads(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<V>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Получает список значений в мутабельном словаре, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},['1'])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictPayloads($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```

## MutDictItems {#mutdictitems}

#### Сигнатура

```yql
MutDictItems(Linear<mutDictType for Dict<K,V>>)->Tuple<Linear<mutDictType for Dict<K,V>>,List<Tuple<K,V>>>
```

Функция доступна начиная с версии [2025.04](../changelog/2025.04.md).
Получает список кортежей с парами ключ-значение в мутабельном словаре, возвращает этот же мутабельный словарь и результат.

#### Примеры

```yql
SELECT Block(
    ($arg)->{
        $dict = ToMutDict({'foo':1}, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({'foo':1},[('foo',1)])

SELECT Block(
    ($arg)->{
        $dict = MutDictCreate(String, Int32, $arg);
        $dict, $val = MutDictItems($dict);
        return (FromMutDict($dict), $val);
    }); -- ({},[])
```
