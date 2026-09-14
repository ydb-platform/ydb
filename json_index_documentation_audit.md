# Функциональный аудит JSON-индексов YDB

Дата проверки: 2026-09-12.

Проверенная ревизия:

- commit: `35fe6b99b6b321ebc724e0c644432b2301f49da6`;
- branch: `main`;
- сервер и CLI: обычная `relwithdebinfo`-сборка без санитайзера, согласно
  обновлённому условию аудита;
- локальная одновузловая база `/local`;
- включены `enable_json_index` и `enable_json_index_auto_select`.

## Область проверки

Проверка опиралась на следующие страницы документации:

- [JSON-индексы](ydb/docs/ru/core/dev/json-indexes.md);
- [CREATE TABLE: JSON-индекс](ydb/docs/ru/core/yql/reference/syntax/create_table/json_index.md);
- [VIEW: JSON-индекс](ydb/docs/ru/core/yql/reference/syntax/select/json_index.md);
- [JSON-индекс — быстрый старт](ydb/docs/ru/core/recipes/json-search/json-index-quickstart.md);
- [Каталог со вложенными атрибутами](ydb/docs/ru/core/recipes/json-search/json-index-catalog.md);
- [Параметризованные запросы](ydb/docs/ru/core/recipes/json-search/json-index-parameters.md);
- [Проверка типа поля и наличия пути](ydb/docs/ru/core/recipes/json-search/json-index-typecheck.md).

Проверялись:

- `Json` и `JsonDocument`;
- inline-создание индекса;
- явное чтение через `VIEW` и автоматический выбор оптимизатором;
- `JSON_EXISTS`, `JSON_VALUE`, `type()`, `size()`, JSON `false` и JSON `null`;
- `INSERT`, `UPSERT`, `REPLACE`, `UPDATE` и `DELETE`;
- атомарность неуспешного многострочного `INSERT`;
- optional-параметры и числовая граница `2^53`;
- строковый и составной первичный ключ;
- prefixed JSON-индекс;
- воспроизводимость результата `SHOW CREATE TABLE`;
- read-your-writes через синхронный JSON-индекс.

## Краткий результат

Найдены:

- **1 проблема реализации** с тремя независимо воспроизводимыми DML-сценариями;
- **3 проблемы документации**;
- срабатывания санитайзеров не исследовались после отмены требования ASAN.

Отдельный regression-target:

`ydb/tests/functional/json_index_doc_audit`

Команда проверки:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/tests/functional/json_index_doc_audit 2>&1 | tail
```

Фактический результат на проверенной ревизии:

```text
6 tests: 6 - FAIL
```

Сборка и style-проверка прошли. Все шесть тестов упали на целевых assertions,
а не во время запуска кластера или подготовки схемы.

## Проблемы реализации

### P1. JSON-индекс нарушает read-your-writes внутри транзакции

Тип: реализация.

Затрагивает:

- `Json`;
- `JsonDocument`;
- `INSERT`/`UPSERT`;
- `UPDATE` индексируемой JSON-колонки;
- `DELETE`.

#### Ожидаемое поведение

JSON-индекс объявлен глобальным синхронным индексом. В одной serializable
read-write транзакции чтение после собственной записи должно видеть текущую
версию данных:

- после вставки — новую строку;
- после обновления — новое значение и не видеть старое;
- после удаления — не видеть удалённую строку.

Так ведёт себя обычный синхронный `GLOBAL` secondary index в эквивалентном
контрольном сценарии. В кодовой базе эта гарантия также явно зафиксирована в
`ydb/core/kqp/ut/tx/kqp_read_your_writes_ut.cpp` тестами
`InsertThenSelectByIndex`, `UpdateIndexedColumnThenSelectByIndex` и
`DeleteThenSelectByIndex`.

#### Фактическое поведение

`INSERT`/`UPSERT` и последующий `SELECT ... VIEW json_idx` в одном запросе:

```yql
UPSERT INTO documents (id, payload) VALUES
    (1, JsonDocument(@@{"state":"inserted"}@@));

SELECT id FROM documents VIEW json_idx
WHERE JSON_VALUE(payload, '$.state' RETURNING Utf8) = "inserted"u;
```

возвращает:

```text
[]
```

Эквивалентный запрос через обычный синхронный индекс возвращает только что
добавленный ключ.

После `UPDATE` JSON-индекс внутри той же транзакции продолжает возвращать старое
значение и не находит новое. После `DELETE` он продолжает возвращать удалённую
строку. После commit отдельный запрос видит корректное состояние, то есть
нарушение ограничено read-your-writes, а персистентное обслуживание индекса
работает.

#### Регрессионные тесты

Файл:
[test_json_index_doc_audit.py](ydb/tests/functional/json_index_doc_audit/test_json_index_doc_audit.py)

- `test_insert_then_select_by_json_index_in_same_transaction[Json]`;
- `test_insert_then_select_by_json_index_in_same_transaction[JsonDocument]`;
- `test_update_then_select_by_json_index_in_same_transaction[Json]`;
- `test_update_then_select_by_json_index_in_same_transaction[JsonDocument]`;
- `test_delete_then_select_by_json_index_in_same_transaction[Json]`;
- `test_delete_then_select_by_json_index_in_same_transaction[JsonDocument]`.

Фактические расхождения:

```text
insert: assert [] == [1]
update: assert [] == [1]
delete: assert [{'id': 1}] == []
```

## Проблемы документации

Для проблем документации отдельные regression-тесты не добавлялись.

### D1. В обзоре указан невалидный порядок `RETURNING` и `PASSING`

Тип: документация RU и EN.

В [русском обзоре](ydb/docs/ru/core/dev/json-indexes.md) и соответствующей
английской странице приведена форма:

```yql
JSON_VALUE(doc, '$.x ? (@.y == $v)' RETURNING Int64 PASSING 42 AS v)
```

Она отклоняется парсером:

```text
mismatched input 'PASSING' expecting {')', DEFAULT, ERROR, NULL}
```

Рабочий порядок, который правильно используется в рецепте:

```yql
JSON_VALUE(doc, '$.x ? (@.y == $v)' PASSING 42 AS v RETURNING Int64)
```

### D2. Ограничение первичного ключа устарело

Тип: документация.

[Обзор](ydb/docs/ru/core/dev/json-indexes.md) и
[quickstart](ydb/docs/ru/core/recipes/json-search/json-index-quickstart.md)
безусловно утверждают, что первичный ключ должен состоять из одной колонки типа
`Uint64`, `Uint32`, `Int64` или `Int32`.

На текущем `main` без дополнительного включения row-id feature flags успешно
создаются и работают JSON-индексы для:

- единственного PK типа `Utf8`;
- составного PK `(Uint64, Utf8)`.

Для строкового PK сервер автоматически создал:

```text
__ydb_row_id         Uint64
__ydb_unique_row_id  GlobalUnique (__ydb_row_id)
json_idx             GlobalJson (payload)
```

Запись без явного `__ydb_row_id` и последующий поиск через JSON-индекс прошли
успешно. Документации необходимо либо описать row-id режим, либо явно связать
старое ограничение с версиями и feature flags, где оно ещё действует.

### D3. Не документирован prefixed JSON-индекс

Тип: документация.

[Справка CREATE TABLE](ydb/docs/ru/core/yql/reference/syntax/create_table/json_index.md)
показывает только:

```yql
INDEX json_idx GLOBAL USING json ON (json_column)
```

и говорит, что индекс строится только по одной колонке. На текущем `main`
успешно создаётся и работает форма:

```yql
INDEX json_idx GLOBAL USING json ON (tenant, payload)
```

Запрос с равенством по prefix:

```yql
SELECT id FROM documents VIEW json_idx
WHERE tenant = 10
  AND JSON_VALUE(payload, '$.kind' RETURNING Utf8) = "wanted"u;
```

вернул ожидаемую строку. Документация не описывает:

- допустимые типы и количество prefix-колонок;
- обязательность условий по prefix при чтении;
- поддерживаемые операции сравнения;
- сочетание prefix с auto-select;
- ограничения DML и построения такого индекса.

## Проверки без обнаруженных расхождений

| Область | Результат |
|---|---|
| Quickstart: `JSON_EXISTS($.user.id)` | Индекс и scan вернули `1, 2, 3` |
| Quickstart: имя `Alice` | Индекс и scan вернули `1` |
| Комбинация `JSON_EXISTS` + bool `JSON_VALUE` | Индекс и scan вернули `1` |
| `type() = array` | Индекс и scan вернули `1` |
| `size() > 0` для массива | Индекс и scan вернули `1` |
| JSON `false` | Индекс и scan вернули `1, 2, 4` |
| JSON `null` | Индекс и scan вернули `4` |
| Автоматический выбор | В плане присутствует `ReadFullTextIndex`, `Index: json_idx` |
| DML после commit | `Json` и `JsonDocument` согласованы с основной таблицей |
| `REPLACE` без JSON-колонки | Старые токены удалены |
| Конфликтующий многострочный `INSERT` | Новая строка и её токены полностью rollback |
| `Optional<Utf8>` как параметр | Отклонён с `Parameter with unsupported type` |
| Значение больше `2^53` | Индекс и scan одинаково отражают документированную потерю точности |
| Строковый и составной PK | Индекс создаётся и корректно читает данные после commit |
| Prefixed index | Корректно фильтрует по prefix и JSON-предикату |
| `SHOW CREATE TABLE` для row-id режима | DDL создал работоспособный клон |

## Вывод

После commit JSON-индекс согласован с основной таблицей в проверенных сценариях,
а базовые предикаты и автоматический выбор соответствуют документации. Главная
проблема реализации находится на транзакционной границе: JSON-индекс не учитывает
собственные изменения текущей транзакции, хотя обычный синхронный secondary index
их учитывает. Кроме того, документация отстаёт от реализованных row-id и prefixed
режимов и содержит неисполняемый пример `JSON_VALUE ... RETURNING ... PASSING`.
