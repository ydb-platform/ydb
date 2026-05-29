# Проверка типа поля и наличия пути

Этот рецепт показывает, как [JSON-индекс](../../dev/json-indexes.md) применяется для проверки структуры документов: наличие конкретного пути, тип значения по пути и т. п. Эти задачи часто встречаются при работе с разнородными JSON-документами, когда часть полей опциональна или может иметь разный тип.

## Подготовка

```yql
CREATE TABLE documents (
    id Uint64,
    payload JsonDocument,
    PRIMARY KEY (id),
    INDEX json_idx GLOBAL USING json ON (payload)
);

UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"data": [1, 2, 3]}@@)),
    (2, JsonDocument(@@{"data": "plain text"}@@)),
    (3, JsonDocument(@@{"data": {"nested": true}}@@)),
    (4, JsonDocument(@@{"meta": "no data field"}@@));
```

## Найти документы, у которых поле содержит массив

Метод JsonPath `.type()` возвращает строковое имя типа значения по указанному пути. Это позволяет фильтровать документы по типу содержимого:

```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u;
```

В индекс попадает токен пути `$.data` — метод `.type()` завершает построение токена пути. Точная проверка строкового значения `"array"` выполняется пост-фильтром.

Результат:

```text
id
1
```

Возможные значения, возвращаемые `.type()`: `"null"`, `"boolean"`, `"number"`, `"string"`, `"array"`, `"object"`.

## Найти документы, у которых поле является не пустым массивом

Метод `.size()` возвращает количество элементов массива (или 1 для скаляра, 0 для отсутствующего пути). В комбинации с фильтром по типу:

```yql
SELECT id
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.data.type()' RETURNING Utf8) = "array"u
  AND JSON_VALUE(payload, '$.data.size()' RETURNING Int64) > 0;
```

Оба фрагмента индексируются по соответствующим путям, а точные значения проверяются пост-фильтром. Результат: `id = 1`.

## Найти документы со значением `false` или `null`

TODO: валидация содержимого ниже @mzinal

Для проверки «значение равно `false`» или «значение равно `null`» используйте JsonPath внутри `JSON_EXISTS`, а не `JSON_VALUE(...) IS NULL`:

```yql
-- Значение поля 'archived' равно false
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.archived ? (@ == false)');

-- Значение поля 'value' равно null
SELECT id
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.value ? (@ == null)');
```

В индекс попадает токен пути `$.archived` (или `$.value`), а точное сравнение с `false` или `null` выполняется пост-фильтром.

{% note info %}

Подробнее об ограничениях `IS NULL` и `JSON_VALUE` с булевым результатом см. в разделе [{#T}](../../dev/json-indexes.md#json-value).

{% endnote %}

## Подробнее

* [JSON_EXISTS](../../dev/json-indexes.md#json-exists) — что допустимо в выражениях JsonPath.
* [JSON_VALUE](../../dev/json-indexes.md#json-value) — что допустимо при извлечении значений.
* [JsonPath: методы](../../yql/reference/builtins/json.md#jsonpath) — список методов (`type`, `size`, `keyvalue`, ...) и предикатов JsonPath.
