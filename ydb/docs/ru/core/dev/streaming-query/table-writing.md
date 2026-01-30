# Запись в таблицы

Запись в таблицы позволяет сохранять результаты потокового запроса для последующего анализа обычными SQL-запросами. Например, можно агрегировать события из потока и сохранять итоги в таблицу.

Для записи используется [UPSERT INTO](../../yql/reference/syntax/upsert_into) — вставка новой строки или обновление существующей по первичному ключу.

{% note alert %}

Не поддерживаются:

- команда [INSERT INTO](../../yql/reference/syntax/insert_into);
- запись в таблицы {{ ydb-short-name }}, находящиеся во внешних БД.

{% endnote %}

## Пример

Запрос читает события из топика и записывает их в таблицу `output_table`. Поле `Ts` преобразуется из строки в тип `Timestamp`.

```sql
CREATE STREAMING QUERY query_with_table_write AS
DO BEGIN

-- Чтение из топика и запись в таблицу
UPSERT INTO
    output_table
SELECT
    -- Преобразование строки в Timestamp
    Unwrap(CAST(Ts AS Timestamp)) AS Ts,
    Country,
    Count
FROM
    -- Чтение событий из топика
    ydb_source.input_topic
WITH (
    -- Формат данных в топике
    FORMAT = json_each_row,
    -- Схема данных
    SCHEMA = (
        Ts String NOT NULL,
        Count Uint64 NOT NULL,
        Country Utf8 NOT NULL
    )
)

END DO
```
