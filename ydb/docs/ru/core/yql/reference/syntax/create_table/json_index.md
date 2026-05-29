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
* `SYNC` — указывает на синхронное обновление индекса. Это единственный доступный для JSON-индексов режим, явно указывать не обязательно.
* `<json_column>` — одна колонка таблицы типа `Json` или `JsonDocument` (на данный момент в индекс может быть включена только одна колонка).

JSON-индекс не поддерживает выражение `COVER` — попытка указать его приведёт к ошибке.

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

## Пример

```yql
CREATE TABLE documents (
    id Uint64 NOT NULL,
    payload JsonDocument NOT NULL,
    INDEX json_idx GLOBAL USING json ON (payload),
    PRIMARY KEY (id)
)
```

В этом примере создаётся таблица `documents` с JSON-индексом `json_idx` по колонке `payload`. Индекс будет использоваться запросами, в предикате `WHERE` которых содержатся вызовы `JSON_EXISTS` или `JSON_VALUE` по колонке `payload`.
