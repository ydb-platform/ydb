# JSON-индекс

{% if backend_name == 'YDB' %}[JSON-индексы](../../../../dev/json-indexes.md){% else %}JSON-индексы{% endif %} в {% if backend_name == 'YDB' %}[строковых](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}строковых{% endif %} таблицах создаются с помощью того же синтаксиса, что и [вторичные индексы](secondary_index.md), при указании `json` в качестве типа индекса. Подмножество доступного для JSON-индексов синтаксиса:

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING json
        ON ( [<prefix_columns>,] <json_column> )
    [,   ...]
)
```

Где:

* `<index_name>` — уникальное имя индекса для доступа к данным.
* `SYNC` — указывает на синхронное обновление индекса. Это единственный доступный для JSON-индексов режим, явно указывать не обязательно.
* `<prefix_columns>` — необязательные колонки фильтрации для [JSON-индекса с фильтрацией](../../../../dev/json-indexes.md#filtered). Их типы должны быть допустимы в первичном ключе; вместе эти колонки не должны включать весь первичный ключ таблицы.
* `<json_column>` — одна колонка типа `Json` или `JsonDocument`, указанная последней в `ON`.

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

## Пример с колонкой фильтрации {#filtered}

Чтобы искать в документах конкретного пользователя, укажите `user_id` перед JSON-колонкой:

```yql
CREATE TABLE documents (
    id Uint64 NOT NULL,
    user_id Uint64,
    payload JsonDocument,
    INDEX json_idx GLOBAL USING json ON (user_id, payload),
    PRIMARY KEY (id)
);
```

В запросе к такому индексу нужно указать `user_id = <значение>` вместе с условием по JSON. См. [пример запроса](../select/json_index.md#filtered).
