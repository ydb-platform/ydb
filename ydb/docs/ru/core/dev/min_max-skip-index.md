# min_max-индекс

min_max-индекс — [локальный индекс](../concepts/glossary.md#local-index), который ускоряет сканирующие запросы с высокоселективным фильтром за счёт пропуска фрагментов. В отличие от глобальных [вторичных индексов](../concepts/glossary.md#secondary-index), он работает как фильтр чтения основной таблицы и уменьшает объём данных, которые нужно фактически прочитать.

Для каждого индексируемого фрагмента данных min_max-индекс хранит минимальное и максимальное значение одной колонки. Во время выполнения запроса {{ ydb-short-name }} вычисляет предикат на этих двух значениях. Если из результатов вычисления следует, что предикат отфильтрует все кортежи фрагмента, фрагмент пропускается.

## Примеры {#examples}

Синтаксис создания: [CREATE TABLE](../yql/reference/syntax/create_table/min_max_index.md), [ALTER TABLE ADD INDEX](../yql/reference/syntax/alter_table/indexes.md#local-min-max).

Создание колоночной таблицы с min_max-индексом:

```yql
CREATE TABLE events (
    id Uint64,
    created_at Timestamp,
    level Int32,
    resource_id Utf8,
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

Добавление min_max-индекса к существующей колоночной таблице:

```yql
ALTER TABLE events
  ADD INDEX idx_resource_id LOCAL USING min_max
  ON (resource_id);
```

Запросы с диапазонными предикатами могут использовать индекс для пропуска неподходящих фрагментов:

```yql
SELECT id, resource_id
FROM events
WHERE created_at BETWEEN Timestamp("2024-01-01T00:00:00.000000Z")
                     AND Timestamp("2024-01-02T00:00:00.000000Z");
```

## Когда применять {#use}

min_max-индекс полезен, когда значения индексируемой колонки слабо меняются между соседними по первичному ключу строками таблицы: например, это временные метки, монотонно растущие идентификаторы или другие значения, коррелированные с первичным ключом.

Также min_max-индекс может быть полезен, когда предикат фильтра выбирает очень малую долю данных (порядка одной строки на миллион). Например, при запросах над таблицей логов сервиса, оставляющих только записи с уровнем `ERROR`: если сервис пишет одну ошибку на 1 000 000 записей, min_max-индекс, вероятно, существенно сократит объём чтения.

## Особенности и ограничения {#limitations}

{% include [min_max_index_features.md](../yql/reference/syntax/_includes/min_max_index_features.md) %}

{% note info "Ограничения" %}

{% include [min_max_index_limitations.md](../yql/reference/syntax/_includes/min_max_index_limitations.md) %}

{% endnote %}

## Дополнительные материалы {#see-also}

* [Вторичные индексы](secondary-indexes.md)
* [Локальные индексы](../concepts/query_execution/local_indexes.md)
* [Справочник YQL: CREATE TABLE](../yql/reference/syntax/create_table/min_max_index.md)
* [Справочник YQL: SELECT](../yql/reference/syntax/select/index.md)
* [Справочник YQL: ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md#local-min-max)
* [Быстрый старт](../recipes/min_max-skip-index/min_max-skip-index-quickstart.md)
