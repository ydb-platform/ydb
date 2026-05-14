# Блум-индекс — быстрый старт

Ниже минимальный пример: колоночная таблица с первичным ключом и локальным индексом `bloom_filter` по колонке, которая часто используется в условиях фильтрации.

```yql
CREATE TABLE `/Root/events` (
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

Для строковых колонок можно использовать `bloom_ngram_filter`:

```yql
ALTER TABLE `/Root/events`
  ADD INDEX idx_msg LOCAL USING bloom_ngram_filter
  ON (message)
  WITH (
    ngram_size = 3,
    false_positive_probability = 0.01,
    case_sensitive = true
  );
```

## Запросы и эффект

После загрузки данных селективные запросы с условиями по проиндексированным колонкам могут читать меньше данных: при обходе хранилища блум-индекс позволяет пропускать фрагменты, в которых искомых значений с высокой вероятностью нет (по сравнению с полным чтением колонки без такого фильтра).

Пример данных и запросов к таблице из примеров выше:

```yql
INSERT INTO `/Root/events` (id, resource_id, payload, message) VALUES
    (1, "res-1", "{}", "started"),
    (2, "res-42", "{}", "error: timeout"),
    (3, "res-2", "{}", "done");
```

Фильтр по значению в колонке с `bloom_filter` — движок может отсечь лишние фрагменты при чтении `resource_id` и связанных колонок:

```yql
SELECT id, message
FROM `/Root/events`
WHERE resource_id = "res-42";
```

Поиск подстроки в колонке с `bloom_ngram_filter` — индекс по n-граммам помогает отбросить фрагменты без подходящих n-грамм в `message`:

```yql
SELECT id, message
FROM `/Root/events`
WHERE message LIKE '%timeout%';
```

Дополнительные материалы:

* подробности и ограничения — в статье [Блум-индексы](../../dev/bloom-skip-indexes.md);
* настройка параметров — раздел [Настройка параметров](../../dev/bloom-skip-indexes.md#tuning);
* полный синтаксис — в [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom).
