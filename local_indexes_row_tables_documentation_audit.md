# Функциональный аудит локальных индексов строковых таблиц

Дата проверки: 2026-09-12.

Проверяемая версия: `main`, commit `35fe6b99b6b321ebc724e0c644432b2301f49da6`.

Область проверки: только строковые таблицы. Поэтому функционально проверялся
`LOCAL USING bloom_filter`; `bloom_ngram_filter` и `min_max` проверялись только
на корректное отклонение, поскольку документация поддерживает их лишь для
колоночных таблиц. Баги в трекере не создавались. ASAN не использовался.

## Итог

Найдена одна проблема реализации и одна проблема документации.

| ID | Тип | Краткое описание | Regression test |
|---|---|---|---|
| P1 | реализация | Документированный `ALTER INDEX` параметров row bloom index не работает | `test_alter_false_positive_probability_on_row_bloom_index` — падает |
| D1 | документация | Основной пример `bloom_filter` для строковой таблицы использует non-PK колонку и не выполняется | не нужен |

## P1. Нельзя изменить параметры row bloom index

Документация `bloom-skip-indexes.md` утверждает, что параметры bloom index после
создания меняются через `ALTER INDEX`. Раздел `ALTER TABLE` также перечисляет
`FALSE_POSITIVE_PROBABILITY` как изменяемый параметр локальных bloom indexes без
исключения строковых таблиц.

Валидная команда для существующего row prefix bloom index:

```yql
ALTER TABLE `row_bloom_alter`
ALTER INDEX idx_tenant SET (
    false_positive_probability = 0.5
);
```

отклоняется:

```text
GENERIC_ERROR: Only index with one impl table is supported
```

`SHOW CREATE TABLE` после ошибки сохраняет старое значение `0.01`. Сообщение
указывает на то, что общий путь `ALTER INDEX` рассчитан на индекс с отдельной
implementation table, которой у локального row bloom index нет.

### Regression test

Добавлен тест
`ydb/tests/functional/local_index_doc_audit/test_local_index_doc_audit.py::TestLocalIndexDocumentationAudit::test_alter_false_positive_probability_on_row_bloom_index`.

Тест создаёт допустимый bloom index по левому префиксу составного PK и выполняет
документированный `ALTER INDEX`. На проверяемой версии он ожидаемо падает с
указанным сообщением.

Команда проверки:

```bash
./ya make --build relwithdebinfo -tA \
  ydb/tests/functional/local_index_doc_audit \
  -F '*test_alter_false_positive_probability_on_row_bloom_index*' \
  2>&1 | tail
```

Результат: `1 test: 1 - FAIL`.

## D1. Документированный CREATE TABLE невалиден для row store

В `ydb/docs/ru/core/dev/bloom-skip-indexes.md` общий пример создаёт таблицу без
`STORE = COLUMN`, то есть строковую, с primary key `(id)` и индексом:

```yql
INDEX idx_bloom LOCAL USING bloom_filter ON (resource_id)
```

Этот пример буквально не выполняется:

```text
Bloom filter column 'resource_id' does not match PK column 'id' at position 0
```

Причина корректно описана ниже на той же странице: для строковой таблицы
индексируемые колонки обязаны образовывать непрерывный левый префикс primary key.
Пример нужно либо сделать колоночным через `STORE = COLUMN`, либо заменить PK на
начинающийся с `resource_id`, либо пометить его как пример только для column
store. Тест не добавлялся, так как это дефект документации.

## Что прошло

### Допустимые row bloom indexes

- Таблица с PK `(tenant, id)` успешно создана с двумя индексами: по `(tenant)` и
  по полному `(tenant, id)`.
- После загрузки 5000 строк точечные запросы, диапазоны и `IN` вернули ожидаемый
  результат; отсутствующие ключи не дали false negative или лишних строк в
  итоговом результате.
- `UPDATE`, `DELETE` и `UPSERT` после создания индексов сохранили корректность
  чтения.
- Индекс успешно удалён и повторно создан на уже заполненной таблице.
- `SHOW CREATE TABLE` сохранил тип, список колонок и явно заданную вероятность
  ложноположительного срабатывания.

### Валидация схемы

- Индексы по `(id)`, `(id, tenant)` и `(tenant, id, value)` отклонены как не
  являющиеся левым префиксом PK.
- Второй bloom index той же длины префикса отклонён.
- `COVER` отклонён.
- Значения `false_positive_probability` 0 и 1 отклонены с указанием диапазона
  `(0, 1)`.
- `bloom_ngram_filter` на строковой таблице отклонён с ясным сообщением, что он
  поддерживается только для column tables.
- `min_max` на строковой таблице отклонён с таким же явным сообщением.

## Неприменимость min_max в этом аудите

Документация последовательно указывает, что `min_max` поддерживается только для
колоночных таблиц. Поэтому проверки min/max ranges, `NULL`, compaction и pruning
не входят в row-only scope и не могут быть честно выполнены здесь. Ошибку
реализации в отклонении row min_max не фиксируем — это ожидаемое ограничение.

## Ограничения аудита

- Фактическую долю пропущенных storage pages нельзя уверенно определить из
  обычного logical `EXPLAIN`: план показывает lookup/scan основной таблицы, но не
  факт применения конкретного локального bloom block.
- Не выполнялся принудительный compaction с проверкой внутренних counters.
- False positive является допустимым свойством bloom filter; проверялась
  корректность конечного результата, а не гарантированное отсутствие лишних
  storage reads.

## Открытые вопросы

1. Планируется ли поддержать `ALTER INDEX` для row prefix bloom, или документация
   должна явно ограничить эту операцию column bloom indexes?
2. Нужны ли в query stats отдельные counters «local index blocks checked/skipped»,
   чтобы пользователь мог проверить эффективность без сравнения wall-clock time?
3. Нужно ли валидировать противоречивые примеры документации автоматическим
   исполнением для обоих значений `STORE`?
