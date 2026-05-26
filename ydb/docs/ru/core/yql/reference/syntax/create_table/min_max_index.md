# min_max индекс

{% if backend_name == 'YDB' %}[`min_max` индекс](../../../../dev/min-max-index.md){% else %}`min_max` индекс{% endif %} — локальный индекс пропуска для [колоночных таблиц](../../../../concepts/datamodel/table.md#column-oriented-tables), хранящий минимальное и максимальное значение индексируемой колонки для каждого фрагмента данных. Создаётся в секции `INDEX` (по аналогии с [вторичным индексом](secondary_index.md)), но с обязательным `LOCAL` и `USING min_max`.

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        LOCAL
        USING min_max
        ON ( <column> )
    [,   ...]
)
WITH (STORE = COLUMN);
```

Где:

* `<index_name>` — имя индекса.
* `LOCAL` — обязательное ключевое слово; `min_max` доступен только как локальный индекс.
* `<column>` — имя одной колонки, по которой строится индекс. Указание нескольких колонок в `ON (...)` не поддерживается.
* `min_max` не имеет настраиваемых параметров — секция `WITH (...)` для него не указывается.
* Колонки покрытия (`COVER (...)`) для `min_max` не поддерживаются.

Поддерживаемые типы колонок и предикаты, ускоряемые индексом, описаны в разделе [min_max индекс](../../../../dev/min-max-index.md). Не поддерживаются типы без определённого порядка сравнения: `Json` и `JsonDocument`.

Создание `min_max` индекса для уже существующей таблицы выполняется через [`ALTER TABLE ADD INDEX`](../alter_table/indexes.md).

## Примеры

### Индекс по временной метке

```yql
CREATE TABLE events (
    id Uint64 NOT NULL,
    event_time Timestamp NOT NULL,
    payload String,
    PRIMARY KEY (id),
    INDEX idx_event_time LOCAL USING min_max ON (event_time)
)
WITH (STORE = COLUMN);
```

### Индекс по строковой колонке

```yql
CREATE TABLE events (
    id Uint64 NOT NULL,
    source Utf8,
    payload String,
    PRIMARY KEY (id),
    INDEX idx_source LOCAL USING min_max ON (source)
)
WITH (STORE = COLUMN);
```
