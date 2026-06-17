# Блум-индекс — быстрый старт

## Создание таблицы с индексом bloom_filter

Ниже минимальный пример: колоночная таблица с первичным ключом и локальным индексом `bloom_filter` по колонке, которая часто используется в условиях фильтрации.

```yql
CREATE TABLE events (
    id Uint64 NOT NULL,
    resource_id Utf8 NOT NULL,
    payload String,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_res LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
)
WITH (
    STORE = COLUMN
);
```

## Расширение примера: n-граммный индекс

К той же таблице из примера выше можно добавить `bloom_ngram_filter` по строковой колонке (для колоночных таблиц):

```yql
ALTER TABLE events
  ADD INDEX idx_msg LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

## Запросы и эффект

После загрузки данных селективные запросы с условиями по проиндексированным колонкам могут читать меньше данных: при обходе хранилища Блум-индекс пропускает фрагменты, в которых искомое значение гарантированно отсутствует (по сравнению с полным чтением колонки без такого фильтра).

Пример данных и запросов к таблице из примеров выше:

```yql
INSERT INTO events (id, resource_id, payload, message) VALUES
    (1, "res-1", "{}", "started"),
    (2, "res-42", "{}", "error: timeout"),
    (3, "res-2", "{}", "done");
```

Фильтр по значению в колонке с `bloom_filter` — движок может отсечь лишние фрагменты при чтении `resource_id` и связанных колонок:

```yql
SELECT id, message
FROM events
WHERE resource_id = "res-42";
```

Поиск подстроки в колонке с `bloom_ngram_filter` — индекс по n-граммам помогает отбросить фрагменты без подходящих n-грамм в `message`:

```yql
SELECT id, message
FROM events
WHERE message LIKE '%timeout%';
```

## Как убедиться в эффективности индекса

Чтобы проверить, что Блум-индекс действительно сокращает объём чтения, выполните один и тот же селективный запрос на таблице с достаточным объёмом данных и сравните статистику:

* В {{ ydb-short-name }} CLI добавьте флаг `--stats full` к команде `yql` или `table query execute` — в выводе будет видно количество прочитанных строк и байт.
* В системе мониторинга сравните метрику `table.datashard.read.rows` до и после создания индекса.

Дополнительные материалы:

* подробности и ограничения — в статье [Блум-индексы](../../dev/bloom-skip-indexes.md);
* настройка параметров — раздел [Настройка параметров](../../dev/bloom-skip-indexes.md#tuning);
* полный синтаксис — в [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom).
