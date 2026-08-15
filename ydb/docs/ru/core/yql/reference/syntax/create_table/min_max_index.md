# min_max-индекс

{% if backend_name == 'YDB' %}[min_max-индекс](../../../../dev/min_max-skip-index.md){% else %}min_max-индекс{% endif %} — [локальный индекс](../../../../concepts/glossary.md#local-index), его можно задать только с ключевым словом `LOCAL`. При создании таблицы в секции `INDEX` используется тип `min_max` (по аналогии с [вторичным индексом](secondary_index.md), но с обязательным `LOCAL` и соответствующим `USING`). См. также [локальные индексы](../../../../concepts/query_execution/local_indexes.md).

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        LOCAL
        USING min_max
        ON ( <index_column> )
    [,   ...]
)
```

Где:

* `<index_name>` — имя индекса.
* `LOCAL` — обязательное ключевое слово для min_max-индекса.
* `<index_column>` — колонка, по которой строится индекс. Нужно указать ровно одну колонку.
* Для min_max-индекса не поддерживаются колонки покрытия (`COVER (...)`) и дополнительные колонки данных.

Параметры `WITH (...)`:

{% include [min_max_index_parameters.md](../_includes/min_max_index_parameters.md) %}

Создание min_max-индекса для уже существующей таблицы описано в разделе [`ALTER TABLE ADD INDEX`](../alter_table/indexes.md#local-min-max).

## Пример {#example}

```yql
CREATE TABLE events (
    id Uint64,
    created_at Timestamp,
    level Int32,
    PRIMARY KEY (id),
    INDEX idx_created_at LOCAL USING min_max
        ON (created_at),
    INDEX idx_level LOCAL USING min_max
        ON (level)
)
WITH (
    STORE = COLUMN
);
```
