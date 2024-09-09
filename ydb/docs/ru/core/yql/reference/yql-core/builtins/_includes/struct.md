# Функции для работы со структурами

## TryMember {#trymember}

Попытка получить значение поля из структуры, а в случае его отсутствия среди полей или null в значении структуры использовать значение по умолчанию.
Тип `default_value` должен совпадать с типом поля `key` из структуры.

**Сигнатура**
```
TryMember(struct:Struct<...>, key:String, default_value:T) -> T
TryMember(struct:Struct<...>?, key:String, default_value:T) -> T?
```

Аргументы:

1. struct - исходная структура;
2. key - имя поля;
3. default_value - значение по умолчанию если поле отсутствует

{% note info "Ограничение" %}

Имя поля (key) не может зависеть от данных или от аргументов лямбды. В этом случае функция TryMember не может быть протипизирована.

{% endnote %}

**Примеры**
``` yql
$struct = <|a:1|>;
SELECT
  TryMember(
    $struct,
    "a",
    123
  ) AS a, -- 1
  TryMember(
    $struct,
    "b",
    123
  ) AS b; -- 123
```

## ExpandStruct {#expandstruct}

Добавление одного или нескольких новых полей в структуру. Возвращается новая раширенная структура.  В случае возникновения дублей в наборе полей будет возвращена ошибка.

**Сигнатура**
```
ExpandStruct(struct:Struct<...>, value_1:T1 AS key_1:K, value_2:T2 AS key_2:K, ....) -> Struct<...>
```

Аргументы:

* В первый аргумент передается исходная структура для расширения.
* Все остальные аргументы должны быть именованными, каждый аргумент добавляет новое поле и имя аргумента используется в роли имени поля (по аналогии с [AsStruct](../basic.md#asstruct)).

**Примеры**
``` yql
$struct = <|a:1|>;
SELECT
  ExpandStruct(
    $struct,
    2 AS b,
    "3" AS c
  ) AS abc;  -- ("a": 1, "b": 2, "c": "3")
```

## AddMember {#addmember}

Добавление одного нового поля в структуру. Если необходимо добавление нескольких полей, предпочтительнее использовать [ExpandStruct](#expandstruct).

В случае возникновения дублей в наборе полей будет возвращена ошибка.

**Сигнатура**
```
AddMember(struct:Struct<...>, new_key:String, new_value:T) -> Struct<...>
```

Аргументы:

1. struct - исходная структура;
2. new_key - имя нового поля;
3. new_value - значение нового поля.

**Примеры**
``` yql
$struct = <|a:1|>;
SELECT
  AddMember(
    $struct,
    "b",
    2
  ) AS ab; -- ("a": 1, "b": 2)
```

## RemoveMember {#removemember}

Удаление поля из структуры. Если указанного поля не существовало, будет возвращена ошибка.

**Сигнатура**
```
RemoveMember(struct:Struct<...>, key_to_delete:String) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Имя поля для удаления

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  RemoveMember(
    $struct,
    "b"
  ) AS a; -- ("a": 1)
```

## ForceRemoveMember {#forceremovemember}

Удаление поля из структуры.

Если указанного поля не существовало, в отличии от [RemoveMember](#removemember) ошибка возвращена не будет.

**Сигнатура**
```
ForceRemoveMember(struct:Struct<...>, key_to_delete:String) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Имя поля для удаления.

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  ForceRemoveMember(
    $struct,
    "c"
  ) AS ab; -- ("a": 1, "b": 2)
```

## ChooseMembers {#choosemembers}

Выделение из структуры полей с заданными именами. Возвращается новая структура только из заданных полей.

Если какого-либо из полей не существовало, будет возвращена ошибка.

**Сигнатура**
```
ChooseMembers(struct:Struct<...>, list_of_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Список имен полей.

**Примеры**
``` yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  ChooseMembers(
    $struct,
    ["a", "b"]
  ) AS ab; -- ("a": 1, "b": 2)
```

## RemoveMembers {#removemembers}

Исключение из структуры полей с заданными именами.

Если какого-либо из полей не существовало, будет возвращена ошибка.

**Сигнатура**
```
RemoveMembers(struct:Struct<...>, list_of_delete_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Список имен полей.

**Примеры**
``` yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  RemoveMembers(
    $struct,
    ["a", "b"]
  ) AS c; -- ("c": 3)
```

## ForceRemoveMembers {#forceremovemembers}

Исключение из структуры полей с заданными именами.

Если какого-либо из полей не существовало, то оно игнорируется.

**Сигнатура**
```
ForceRemoveMembers(struct:Struct<...>, list_of_delete_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Список имен полей.

**Примеры**
``` yql
$struct = <|a:1, b:2, c:3|>;
SELECT
  ForceRemoveMembers(
    $struct,
    ["a", "b", "z"]
  ) AS c; -- ("c": 3)
```

## CombineMembers {#combinemembers}

Объединение полей нескольких структур в новую структуру.

В случае возникновения дублей в результирующем наборе полей будет возвращена ошибка.

**Сигнатура**
```
CombineMembers(struct1:Struct<...>, struct2:Struct<...>, .....) -> Struct<...>
CombineMembers(struct1:Struct<...>?, struct2:Struct<...>?, .....) -> Struct<...>
```

Аргументы: две и более структуры.

**Примеры**
``` yql
$struct1 = <|a:1, b:2|>;
$struct2 = <|c:3|>;
SELECT
  CombineMembers(
    $struct1,
    $struct2
  ) AS abc; -- ("a": 1, "b": 2, "c": 3)
```

## FlattenMembers {#flattenmembers}

Объединение полей нескольких новых структур в новую структуру с поддержкой префиксов.

В случае возникновения дублей в результирующем наборе полей будет возвращена ошибка.

**Сигнатура**
```
FlattenMembers(prefix_struct1:Tuple<String, Struct<...>>, prefix_struct2:Tuple<String, Struct<...>>, ...) -> Struct<...>
```

Аргументы: два и более кортежа из двух элементов: префикс и структура.

**Примеры**
``` yql
$struct1 = <|a:1, b:2|>;
$struct2 = <|c:3|>;
SELECT
  FlattenMembers(
    AsTuple("foo", $struct1), -- fooa, foob
    AsTuple("bar", $struct2)  -- barc
  ) AS abc; -- ("barc": 3, "fooa": 1, "foob": 2)
```

## StructMembers {#structmembers}

Возвращает неупорядоченный список имен полей (возможно, сняв один уровень опциональности) для единственного аргумента - структуры. Для `NULL` аргумента возвращается пустой список строк.

**Сигнатура**
```
StructMembers(struct:Struct<...>) -> List<String>
StructMembers(struct:Struct<...>?) -> List<String>
StructMembers(NULL) -> []
```

Аргумент: структура

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  StructMembers($struct) AS a, -- ['a', 'b']
  StructMembers(NULL) AS b; -- []
```

## RenameMembers {#renamemembers}

Переименовывает поля в переданной структуре. При этом исходное поле можно переименовать в несколько новых. Все поля, не упомянутые в переименовании как исходные, переносятся в результирующую структуру. Если нет какого-то исходного поля в списке для переименования, выдается ошибка. Для опциональной структуры либо `NULL` таким же является и результат.

**Сигнатура**
```
RenameMembers(struct:Struct<...>, rename_rules:List<Tuple<String, String>>) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Список имен полей в форме списка таплов: исходное имя, новое имя.

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  RenameMembers($struct, [('a', 'c'), ('a', 'e')]); -- (b:2, c:1, e:1)
```

## ForceRenameMembers {#forecerenamemembers}

Переименовывает поля в переданной структуре. При этом исходное поле можно переименовать в несколько новых. Все поля, не упомянутые в переименовании как исходные, переносятся в результирующую структуру. Если нет какого-то исходного поля в списке для переименования, оно игнорируется. Для опциональной структуры либо `NULL` таким же является и результат.

**Сигнатура**
```
ForceRenameMembers(struct:Struct<...>, rename_rules:List<Tuple<String, String>>) -> Struct<...>
```

Аргументы:

1. Исходная структура;
2. Список имен полей в форме списка таплов: исходное имя, новое имя.

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  ForceRenameMembers($struct, [('a', 'c'), ('d', 'e')]); -- (b:2, c:1)
```

## GatherMembers {#gathermembers}

Возвращает неупорядоченный список таплов из имени поля и значения. Для `NULL` аргумента возвращается `EmptyList`. Можно использовать только в тех случаях, когда типы элементов в структуре одинаковы или совместимы. Для опциональной структуры возвращает опциональный список.

**Сигнатура**
```
GatherMembers(struct:Struct<...>) -> List<Tuple<String,V>>
GatherMembers(struct:Struct<...>?) -> List<Tuple<String,V>>?
GatherMembers(NULL) -> []
```

Аргумент: структура

**Примеры**
``` yql
$struct = <|a:1, b:2|>;
SELECT
  GatherMembers($struct), -- [('a', 1), ('b', 2)]
  GatherMembers(null); -- []
```

## SpreadMembers {#spreadmembers}

Создает структуру с заданным списком полей и применяет к ней заданный список исправлений в формате (имя поля, значение). Все типы полей результирующей структуры совпадают, и равны типу значений в списке исправлений с добавленной опциональностью (если еще не были таковыми). Если поле не было упомянуто среди списка редактируемых полей, оно возвращается как `NULL`. Среди всех исправлений по одному полю сохраняется последнее. Если в списке исправлений встречается поле, которого нет в списке ожидаемых полей, выдается ошибка.

**Сигнатура**
```
SpreadMembers(list_of_tuples:List<Tuple<String, T>>, result_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Список таплов: имя поля, значение поля;
2. Список всех возможных имен полей в структуре.

**Примеры**
``` yql
SELECT
  SpreadMembers([('a',1),('a',2)],['a','b']); -- (a: 2, b: null)
```

## ForceSpreadMembers {#forcespreadmembers}

Создает структуру с заданным списком полей и применяет к ней заданный список исправлений в формате (имя поля, значение). Все типы полей результирующей структуры совпадают, и равны типу значений в списке исправлений с добавленной опциональностью (если еще не были таковыми). Если поле не было упомянуто среди списка редактируемых полей, оно возвращается как `NULL`. Среди всех исправлений по одному полю сохраняется последнее. Если в списке исправлений встречается поле, которого нет в списке ожидаемых полей, то это исправление игнорируется.

**Сигнатура**
```
ForceSpreadMembers(list_of_tuples:List<Tuple<String, T>>, result_keys:List<String>) -> Struct<...>
```

Аргументы:

1. Список таплов: имя поля, значение поля;
2. Список всех возможных имен полей в структуре.

**Примеры**
``` yql
SELECT
  ForceSpreadMembers([('a',1),('a',2),('c',100)],['a','b']); -- (a: 2, b: null)
```

## StructUnion, StructIntersection, StructDifference, StructSymmetricDifference

Комбинируют две структуры одним из четырех способов, используя предоставленную функцию для слияния полей с одинаковыми именами:

* `StructUnion` добавляет в результат все поля обеих структур;
* `StructIntersection` — поля, которые есть в обеих структурах;
* `StructDifference` — поля которые есть в left, но которых нет в right;
* `StructSymmetricDifference` — все поля, которые есть только в одной из структур.

**Сигнатуры**
```
StructUnion(left:Struct<...>, right:Struct<...>[, mergeLambda:(name:String, l:T1?, r:T2?)->T])->Struct<...>
StructIntersection(left:Struct<...>, right:Struct<...>[, mergeLambda:(name:String, l:T1?, r:T2?)->T])->Struct<...>
StructDifference(left:Struct<...>, right:Struct<...>)->Struct<...>
StructSymmetricDifference(left:Struct<...>, right:Struct<...>)->Struct<...>
```

Аргументы:

1. `left` - первая структура;
2. `right` - вторая структура;
3. `mergeLambda` - _(опционально)_ позволяет задать функцию для объединения полей (аргументы: имя поля, Optional значение поля в первой структуре, Optional значение поля во второй структуре); по умолчанию выбирается значение поля из первой структуры, а если в первой отсутствует — из второй.

**Примеры**
``` yql
$merge = ($name, $l, $r) -> {
    return ($l ?? 0) + ($r ?? 0);
};
$left = <|a: 1, b: 2, c: 3|>;
$right = <|c: 1, d: 2, e: 3|>;

SELECT
    StructUnion($left, $right),                 -- <|a: 1, b: 2, c: 3, d: 2, e: 3|>
    StructUnion($left, $right, $merge),         -- <|a: 1, b: 2, c: 4, d: 2, e: 3|>
    StructIntersection($left, $right, $merge),  -- <|c: 4|>
    StructDifference($left, $right),            -- <|a: 1, b: 1|>
    StructSymmetricDifference($left, $right)    -- <|a: 1, b: 2, d: 2, e: 3|>
;
```
