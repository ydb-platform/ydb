# Bloom skip индекс — быстрый старт

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

Дополнительные материалы:

* подробности и ограничения — в статье [Bloom skip индексы](../../dev/bloom-skip-indexes.md);
* полный синтаксис — в [ALTER TABLE ADD INDEX](../../yql/reference/syntax/alter_table/indexes.md#local-bloom).
