# JSON-индекс

{% if backend_name == 'YDB' %}[JSON-индексы](../../../../dev/json-indexes.md){% else %}JSON-индексы{% endif %} в {% if backend_name == 'YDB' %}[строковых](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}строковых{% endif %} таблицах создаются с помощью того же синтаксиса, что и [вторичные индексы](secondary_index.md), при указании `json` в качестве типа индекса. Подмножество доступного для JSON-индексов синтаксиса:

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING json
        ON ( <json_column> )
    [,   ...]
)
```

Где:

* `<index_name>` — уникальное имя индекса для доступа к данным.
* `SYNC` — указывает на синхронную запись данных в индекс. Это единственная доступная на данный момент опция, явно указывать её не обязательно.
* `<json_column>` — одна колонка таблицы типа `Json` или `JsonDocument` (поддерживается ровно одна индексируемая колонка).

В отличие от других вторичных индексов, для JSON-индекса:

* первичный ключ таблицы должен состоять из **одной** колонки одного из типов `Uint64`, `Int64`, `Int32`, `Uint32`;
* секция `COVER` (покрывающие колонки) не поддерживается;
* секция `WITH` с дополнительными параметрами не используется.

Подробнее об ограничениях см. в разделе [Ограничения](../../../../dev/json-indexes.md#limitations).

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

## Примеры

### Индекс по колонке типа `JsonDocument`

```yql
CREATE TABLE TestTable (
    Key Uint64,
    Value JsonDocument,
    INDEX json_idx GLOBAL SYNC USING json ON (Value),
    PRIMARY KEY (Key)
)
```

### Индекс по колонке типа `Json`

```yql
CREATE TABLE users (
    id Int64,
    profile Json,
    INDEX profile_idx GLOBAL SYNC USING json ON (profile),
    PRIMARY KEY (id)
)
```
