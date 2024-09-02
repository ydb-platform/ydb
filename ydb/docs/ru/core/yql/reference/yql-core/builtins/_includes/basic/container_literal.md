## Литералы контейнеров {#containerliteral}

Для некоторых контейнеров возможна операторная форма записи их литеральных значений:

* Кортеж — `(value1, value2...)`;
* Структура — `<|name1: value1, name2: value2...|>`;
* Список — `[value1, value2,...]`;
* Словарь — `{key1: value1, key2: value2...}`;
* Множество — `{key1, key2...}`.

Во всех случаях допускается незначащая хвостовая запятая. Для кортежа с одним элементом эта запятая является обязательной - ```(value1,)```.
Для имен полей в литерале структуры допускается использовать выражение, которое можно посчитать в evaluation time, например, строковые литералы, а также идентификаторы (в том числе в backticks).

Для списка внутри используется функция [AsList](../../basic.md#as-container), словаря - [AsDict](../../basic.md#as-container), множества - [AsSet](../../basic.md#as-container), кортежа - [AsTuple](../../basic.md#as-container), структуры - [AsStruct](../../basic.md#as-container).

**Примеры**
``` yql
$name = "computed " || "member name";
SELECT
  (1, 2, "3") AS `tuple`,
  <|
    `complex member name`: 2.3,
    b: 2,
    $name: "3",
    "inline " || "computed member name": false
  |> AS `struct`,
  [1, 2, 3] AS `list`,
  {
    "a": 1,
    "b": 2,
    "c": 3,
  } AS `dict`,
  {1, 2, 3} AS `set`
```
