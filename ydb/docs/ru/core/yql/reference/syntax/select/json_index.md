# VIEW (JSON-индекс)

Для выполнения запроса `SELECT` над строковой таблицей с явным использованием [JSON-индекса](../../../../dev/json-indexes.md) используйте выражение `VIEW`:

```yql
SELECT ...
FROM documents VIEW json_idx
WHERE <предикат на основе JSON_EXISTS / JSON_VALUE>
ORDER BY ...
```

В примере запроса выше `documents` - это имя таблицы, содержащей колонку типа Json или JsonDocument, а `json_idx` - имя созданного над этой колонкой JSON-индекса.

Если предикат не поддерживается для выполнения через JSON-индекс, запрос с указанием выражения `VIEW` завершается ошибкой на этапе компиляции или планирования (сообщение вида «Failed to extract jsonpath tokens from the predicate»). Без выражения `VIEW` запрос с таким предикатом отрабатывает как обычное сканирование таблицы.

{% note info %}

JSON-индекс может быть подставлен [оптимизатором](../../../../concepts/glossary.md#optimizer) автоматически при условии соответствия предиката требованиям для использования индекса. Для отладки и гарантированного использования индекса указывайте его явно с помощью `VIEW IndexName`.

Описание поддерживаемых предикатов см. в разделе [{#T}](../../../../dev/json-indexes.md#predicates).

{% endnote %}

## JSON_EXISTS

[JSON_EXISTS(doc, jsonpath)](../../builtins/json.md#json_exists) проверяет существование пути или значения внутри JSON-документа:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user.id');
```

Также допустимы фильтры JsonPath внутри пути, методы JsonPath (`.type()`, `.size()`) и индексы массивов:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_EXISTS(payload, '$.user ? (@.id > 100)');
```

## JSON_VALUE

[JSON_VALUE(doc, jsonpath RETURNING <type>)](../../builtins/json.md#json_value) извлекает скалярное значение по пути JsonPath и возвращает его в заданном типе. Для использования JSON-индекса секция `RETURNING <type>` обязательна:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u;
```

При проверке равенства в индекс попадает токен «путь + значение», что обеспечивает наибольшую селективность. При остальных сравнениях (`!=`, `<`, `>=`, `BETWEEN`, `IN` и др.) в индекс попадает токен пути, а итоговая точность сравнения обеспечивается пост-фильтром.

### Параметры

Поддерживаются три способа передачи параметров запроса:

1. Прямое сравнение результата `JSON_VALUE` с параметром:

    ```yql
    DECLARE $id AS Int64;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_VALUE(payload, '$.owner_id' RETURNING Int64) = $id;
    ```

2. Проверка наличия результата в списке значений:

    ```yql
    DECLARE $tags AS List<Utf8>;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_VALUE(payload, '$.tag' RETURNING Utf8) IN $tags;
    ```

3. Передача параметра в JsonPath через секцию `PASSING`:

    ```yql
    DECLARE $v AS Int64;
    SELECT * FROM documents VIEW json_idx
    WHERE JSON_EXISTS(payload, '$.k ? (@ == $v)' PASSING $v AS v);
    ```

## Комбинации AND и OR

`JSON_EXISTS` и `JSON_VALUE` над одной JSON-колонкой могут объединяться в одном `WHERE` операторами `AND` и `OR`:

```yql
SELECT id, payload
FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.user.name' RETURNING Utf8) = "Charlie"u
  AND JSON_VALUE(payload, '$.user.id' RETURNING Int64) BETWEEN 100 AND 200;
```

{% note info %}

Выполнение выборки через JSON-индекс может использовать только подмножество выражений на основе `JSON_EXISTS` и `JSON_VALUE` (см. [{#T}](../../../../dev/json-indexes.md#predicates)). Условия, не подпадающие под эти правила (например, отрицания, сравнения с другой колонкой, сравнение двух `JSON_VALUE` из разных колонок), не индексируются; в `AND` они переходят в пост-фильтр, в `OR` приводят к отказу от использования индекса по всей группе условий, объединённой через `OR`.

{% endnote %}
