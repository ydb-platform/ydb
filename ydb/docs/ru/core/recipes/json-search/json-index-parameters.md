# Параметризованные запросы и переменные JsonPath

В большинстве приложений входные данные запроса не подставляются в текст SQL, а передаются через [параметры](../../yql/reference/syntax/declare.md). Типичные варианты использования параметров для работы с [JSON-индексами](../../dev/json-indexes.md):

1. прямое сравнение результата `JSON_VALUE` с параметром;
2. проверка наличия результата в списке значений через выражение `IN`;
3. передача параметра в выражение JsonPath через секцию `PASSING`.

## Подготовка

Ниже приведён пример таблицы, JSON-индекса и заполнения данных.

```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);

UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"owner_id": 100, "tag": "active",  "archived": false}@@)),
    (2, JsonDocument(@@{"owner_id": 100, "tag": "draft",   "archived": false}@@)),
    (3, JsonDocument(@@{"owner_id": 101, "tag": "active",  "archived": true}@@)),
    (4, JsonDocument(@@{"owner_id": 102, "tag": "pending", "archived": false}@@));
```

## Прямое сравнение с параметром

Самый распространённый вариант — параметр подставляется как правый операнд сравнения, а левым операндом является вызов `JSON_VALUE` с явным `RETURNING`:

```yql
DECLARE $owner_id AS Int64;

SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $owner_id
  AND JSON_EXISTS(payload, '$.archived ? (@ == false)');
```

В индекс попадает токен «путь + значение» (`$.owner_id = $owner_id`) — параметр учитывается как обычное значение. Это позволяет выполнить выборку с такой же селективностью, как и при сравнении с литералом.

Запуск с `$owner_id = 100` вернёт строки `1` и `2`.

## Поиск по списку значений

Для поиска по нескольким значениям одного поля удобно использовать `IN`:

```yql
DECLARE $tags AS List<Utf8>;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
```

Поведение индекса различается в зависимости от того, литерал это или параметр:

* `IN ("active"u, "pending"u)` — список литералов превращается в `OR` нескольких токенов «путь + значение», по одному на каждое значение списка.
* `IN $tags` — для каждого значения параметра во время выполнения запроса формируется токен «путь + значение» (`$.tag` + значение из списка). На этапе компиляции значения параметра ещё неизвестны, поэтому в плане запроса (`EXPLAIN`) такое условие отображается как пара «путь + параметр» (`{"path": "$.tag", "param": "$tags"}`).

Параметром для `IN` может выступать любая коллекция скалярных значений: `List<T>`, `Tuple<T, ...>`, `Dict<K, V>` или `Set<T>`.

Запуск с `$tags = ["active"u, "pending"u]` вернёт строки `1`, `3` и `4`.

## Параметры внутри JsonPath (PASSING)

Если параметр должен использоваться внутри фильтра JsonPath (`? (...)`), его передают в секции `PASSING`:

```yql
DECLARE $min_stock AS Int64;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(
    payload,
    '$.warehouses ? (@.stock > $threshold)'
    PASSING $min_stock AS threshold
);
```

Для поиска по индексу используется токен пути `$.warehouses.stock`, а условие `@.stock > $min_stock` проверяется пост-фильтром.

Аналогично `PASSING` работает в `JSON_VALUE`:

```yql
DECLARE $v AS Int64;

SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(
    payload,
    '$.x ? (@.y == $val)' RETURNING Int64
    PASSING $v AS val
) = 10;
```

## Поддерживаемые типы параметров

Для всех трёх способов поддерживаются параметры со следующими типами: `Int8` … `Int64`, `Uint8` … `Uint64`, `Float`, `Double`, `Bytes` (`String`), `Text` (`Utf8`), `Bool`. Опциональные типы (`Optional<T>`) в параметрах не поддерживаются.

Подробнее о типах см. в [{#T}](../../dev/json-indexes.md#json-value).

## Подробнее

* [{#T}](json-index-quickstart.md) — базовый сценарий использования JSON-индекса.
* [Передача параметров в предикаты JSON-индекса](../../dev/json-indexes.md#json-value) — детальное описание всех вариантов.
* [JsonPath](../../yql/reference/builtins/json.md#jsonpath) — синтаксис языка JsonPath.
