# Векторный индекс

{% if backend_name == 'YDB' %}[Векторный индекс](../../../../concepts/glossary.md#vector-index){% else %}векторный индекс{% endif %} в {% if backend_name == 'YDB' %}[строковых](../../../../concepts/datamodel/table.md#row-oriented-tables){% else %}строковых{% endif %} таблицах создаётся с помощью того же синтаксиса, что и [вторичные индексы](secondary_index.md), при указании `vector_kmeans_tree` в качестве типа индекса. Подмножество доступного для векторных индексов синтаксиса:

```yql
CREATE TABLE `<table_name>` (
    ...
    INDEX `<index_name>`
        GLOBAL
        [SYNC]
        USING vector_kmeans_tree
        ON ( <index_columns> )
        [COVER ( <cover_columns> )]
        [WITH ( <parameter_name> = <parameter_value>[, ...])]
    [,   ...]
)
```

Где:

* `<index_name>` - уникальное имя индекса для доступа к данным
* `SYNC` - указывает на синхронную запись данных в индекс. Это единственная доступная на данный момент опция, явно указывать не обязательно.
* `<index_columns>` - список колонок таблицы через запятую, используемых для поиска по индексу (последняя колонка используется как эмбеддинг, остальные - как фильтрующие колонки)
* `<cover_columns>` - список дополнительных колонок создаваемой таблицы, которые будут сохранены в индексе для возможности их извлечения без обращения к основной таблице
* `<parameter_name>` и `<parameter_value>` - список параметров в формате ключ-значение:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

{% note warning %}

{% include [limitations](../../../../_includes/vector-index-update-limitations.md) %}

{% endnote %}

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

## Пример

```yql
CREATE TABLE user_articles (
    article_id Uint64,
    user String,
    title String,
    text String,
    embedding String,
    INDEX emb_cosine_idx GLOBAL SYNC USING vector_kmeans_tree
    ON (user, embedding) COVER (title, text)
    WITH (
        distance="cosine",
        vector_type="float",
        vector_dimension=512,
        clusters=128,
        levels=2
    ),
    PRIMARY KEY (article_id)
)
```
