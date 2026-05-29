# JSON-индекс — быстрый старт

В этом руководстве показано, как создать [JSON-индекс](../../dev/json-indexes.md) и выполнять запросы с использованием функций [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) и [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) в {{ ydb-short-name }}.

## Создайте таблицу и JSON-индекс

```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);
```

Тип колонки `JsonDocument` хранит JSON в компактном бинарном формате и предпочтителен для индексируемой колонки. Альтернативно может использоваться тип `Json` (текстовое представление).

Первичный ключ таблицы должен состоять из единственной колонки типа `Uint64` — это [текущее ограничение](../../dev/json-indexes.md#limitations) реализации JSON-индексов.

## Добавьте тестовые данные

```yql
UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"user": {"id": 100, "name": "Alice"}, "active": true}@@)),
    (2, JsonDocument(@@{"user": {"id": 101, "name": "Bob"}, "active": false}@@)),
    (3, JsonDocument(@@{"user": {"id": 102, "name": "Charlie"}, "archived": true}@@));
```

Здесь конструкция `@@...@@` — это [многострочный строковый литерал](../../yql/reference/syntax/expressions.md), удобный для записи JSON без экранирования кавычек. Функция `JsonDocument(...)` преобразует текст в значение типа `JsonDocument`.

## Фильтр по наличию пути в документе

Функция [JSON_EXISTS](../../yql/reference/builtins/json.md#json_exists) проверяет, существует ли в документе путь, заданный выражением JsonPath.

```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id');
```

Результат:

```bash
id
1
2
3
```

В индекс попадает токен пути `$.user.id`. Индекс возвращает результат без сканирования основной таблицы.

## Отбор строк с конкретным значением поля документа

Функция [JSON_VALUE](../../yql/reference/builtins/json.md#json_value) извлекает скалярное значение по JsonPath; для использования индекса обязательно нужно указать тип возвращаемого значения в секции `RETURNING`:

```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Alice"u;
```

Результат:

```bash
id
1
```

При проверке равенства в индекс попадает токен «путь + значение» (`$.user.name = "Alice"`), что обеспечивает наивысшую селективность.

## Комбинация условий

Несколько вызовов `JSON_EXISTS` / `JSON_VALUE` на одной индексированной JSON-колонке можно объединять операторами `AND` и `OR`:

```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id')
  AND JSON_VALUE(payload, '$.active' RETURNING Bool);
```

Результат:

```bash
id
1
```

## Подробнее

* [JSON-индексы](../../dev/json-indexes.md) — полный обзор возможностей и ограничений.
* [VIEW (JSON-индекс)](../../yql/reference/syntax/select/json_index.md) — синтаксис запросов через `VIEW`.
* [INDEX (CREATE TABLE)](../../yql/reference/syntax/create_table/json_index.md) — синтаксис создания JSON-индекса.
* [{#T}](json-index-catalog.md) — пример каталога товаров с вложенными атрибутами.
