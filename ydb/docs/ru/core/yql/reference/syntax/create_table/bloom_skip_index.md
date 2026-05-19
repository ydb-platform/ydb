# Блум-индекс

{% if backend_name == 'YDB' %}[Блум-индексы](../../../../dev/bloom-skip-indexes.md){% else %}Блум-индексы{% endif %} — [локальные индексы](../../../../concepts/glossary.md#local-index), их можно задать только с ключевым словом `LOCAL`. При создании таблицы в секции `INDEX` используются типы `bloom_filter` или `bloom_ngram_filter` (по аналогии с [вторичным индексом](secondary_index.md), но с обязательным `LOCAL` и соответствующим `USING`). См. также [локальные индексы](../../../../concepts/query_execution/bloom_skip_indexes.md).

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        LOCAL
        USING bloom_filter | bloom_ngram_filter
        ON ( <index_columns> )
        [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [,   ...]
)
```

Где:

* `<index_name>` — имя индекса.
* `LOCAL` — обязательное ключевое слово для Блум-индексов.
* `<index_columns>` — список колонок, по которым строится индекс; количество колонок зависит от типа таблицы и типа индекса.
* Для Блум-индексов не поддерживаются колонки покрытия (`COVER (...)`).

Параметры `WITH (...)`:

{% include [bloom_skip_index_parameters.md](../_includes/bloom_skip_index_parameters.md) %}

Создание и изменение таких индексов для уже существующей таблицы описаны в разделе [`ALTER TABLE ADD INDEX`](../alter_table/indexes.md#local-bloom).

## Примеры

### Индекс `bloom_filter`

```yql
CREATE TABLE events (
    id Uint64,
    resource_id Utf8,
    PRIMARY KEY (id),
    INDEX idx_bloom LOCAL USING bloom_filter
        ON (resource_id)
        WITH (false_positive_probability = 0.01)
);
```

### Индекс `bloom_ngram_filter`

```yql
CREATE TABLE events (
    id Uint64,
    message Utf8,
    PRIMARY KEY (id),
    INDEX idx_ngram LOCAL USING bloom_ngram_filter
        ON (message)
        WITH (
            ngram_size = 3,
            false_positive_probability = 0.01,
            case_sensitive = true
        )
);
```
