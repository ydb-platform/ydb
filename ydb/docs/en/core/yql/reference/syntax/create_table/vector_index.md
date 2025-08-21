# Vector index

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

[Vector index](../../../../concepts/glossary.md#vector-index) in [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) tables is created using the same syntax as [secondary indexes](secondary_index.md), by specifying `vector_kmeans_tree` as the index type. Subset of syntax available for vector indexes:

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

Where:

* `<index_name>` - unique index name for data access
* `SYNC` - indicates synchronous data writing to the index. This is the only currently available option, and it is used by default.
* `<index_columns>` - comma-separated list of table columns used for index searches (the last column is used as embedding, others as filtering columns)
* `<cover_columns>` - list of additional table columns stored in the index to enable retrieval without accessing the main table
* `<parameter_name>` and `<parameter_value>` - list of key-value parameters:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}

{% note warning %}

{% include [limitations](../../../../_includes/vector-index-update-limitations.md) %}

{% endnote %}

## Example

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
