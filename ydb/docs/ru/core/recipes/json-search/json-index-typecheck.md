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

Полный список значений, возвращаемых методом `.type()`, приведён в описании синтаксиса [JsonPath](../../yql/reference/builtins/json.md#jsonpath).

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

В операцию поиска по индексу попадает путь `$.archived` (или `$.value`) и значения `false` или `null`, соответственно.

{% note info %}

Указанные выше условия отбирают документы, в которых указанный атрибут явно установлен в значение false или null. Проверку отсутствия атрибута нельзя выполнить с помощью JSON-индекса.

{% endnote %}

## Подробнее

* [JSON_EXISTS](../../dev/json-indexes.md#json-exists) — что допустимо в выражениях JsonPath.
* [JSON_VALUE](../../dev/json-indexes.md#json-value) — что допустимо при извлечении значений.
* [JsonPath: методы](../../yql/reference/builtins/json.md#jsonpath) — список методов (`type`, `size`, `keyvalue`, ...) и предикатов JsonPath.
