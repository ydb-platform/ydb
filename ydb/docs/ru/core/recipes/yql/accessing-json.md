# Доступ к значениям в JSON с помощью YQL

YQL предоставляет два основных способа извлечения значений из JSON:

- Использование [**JSON-функций из SQL стандарта**](../../yql/reference/builtins/json.md). Этот подход рекомендуется для простых случаев и для команд, которые знакомы с ними по другим СУБД.
- Использование [**Yson UDF**](../../yql/reference/udf/list/yson.md), встроенных функций для работы со [списками](../../yql/reference/builtins/list.md) и [словарями](../../yql/reference/builtins/dict.md), а также [лямбд](../../yql/reference/syntax/expressions.md#lambda). Этот подход более гибкий и тесно интегрирован с системой типов данных {{ ydb-short-name }}, поэтому рекомендуется для сложных случаев.

Ниже приведены рецепты, которые используют один и тот же входной JSON, чтобы показать, как использовать каждый из этих вариантов для проверки существования ключа, получения конкретного значения и извлечения поддерева.

## JSON-функции

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

SELECT
    JSON_EXISTS($json, "$.friends[*].name"), -- True
    CAST(JSON_VALUE($json, "$.friends[0].age") AS Int32), -- 35
    JSON_QUERY($json, "$.friends[0]"); -- {"name": "James Holden", "age": 35}
```

Функции `JSON_*` ожидают на вход данные типа `Json`. В этом примере строковый литерал имеет суффикс `j`, обозначающий его как `Json`. В таблицах данные могут храниться либо в формате JSON, либо как строковое представление. Для преобразования данных из `String` в тип данных `JSON` используйте функцию `CAST`, например `CAST(my_string AS JSON)`.

## Yson UDF

Этот подход обычно сочетает в себе несколько функций и выражений, поэтому запрос может использовать различные конкретные стратегии.

### Преобразование всего JSON в YQL контейнеры

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

$containers = Yson::ConvertTo($json, Struct<friends:List<Struct<name:String?,age:Int32?>>>);
$has_name = ListAny(
    ListMap($containers.friends, ($friend) -> {
        return $friend.name IS NOT NULL;
    })
);
$get_age = $containers.friends[0].age;
$get_first_friend = Yson::SerializeJson(Yson::From($containers.friends[0]));

SELECT
    $has_name, -- True
    $get_age, -- 35
    $get_first_friend; -- {"name": "James Holden", "age": 35}
```

**Не** обязательно преобразовывать весь JSON объект в структурированное сочетание контейнеров. Некоторые поля могут быть опущены, если они не используются, в то время как некоторые поддеревья могут быть оставлены в неструктурированном типе данных, таком как `Json`.

### Работа с представлением в памяти

```yql
$json = @@{
    "friends": [
        {
            "name": "James Holden",
            "age": 35
        },
        {
            "name": "Naomi Nagata",
            "age": 30
        }
    ]
}@@j;

$has_name = ListAny(
    ListMap(Yson::ConvertToList($json.friends), ($friend) -> {
        return Yson::Contains($friend, "name");
    })
);
$get_age = Yson::ConvertToInt64($json.friends[0].age);
$get_first_friend = Yson::SerializeJson($json.friends[0]);

SELECT
    $has_name, -- True
    $get_age, -- 35
    $get_first_friend; -- {"name": "James Holden", "age": 35}
```

## Смотрите также

- [{#T}](modifying-json.md)
