# Vector index

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

{% include [limitations](../../../../_includes/vector_index_limitations.md) %}

{% note warning %}

It makes no sense to create an empty table with a vector index, because for now we don't allow mutations in tables with vector indexes.

You should use `ALTER TABLE ... ADD INDEX` [command](../alter_table/indexes.md)) to add a vector index to an existing table.

{% endnote %}

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
* `SYNC` - indicates synchronous data writes to the index. If not specified, synchronous.
* `<index_columns>` - comma-separated list of table columns used for index searches (the last column is used as embedding, others as prefix columns)
* `<cover_columns>` - list of additional table columns stored in the index to enable retrieval without accessing the main table
* `<parameter_name>` and `<parameter_value>` - list of key-value parameters:

{% include [vector_index_parameters.md](../_includes/vector_index_parameters.md) %}


{% note warning %}

The `distance` and `similarity` parameters can not be specified together.

{% endnote %}


{% note warning %}

Vector indexes with `vector_type=bit` are not currently supported.

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
    WITH (distance="cosine", vector_type="float", vector_dimension=512, clusters=128, levels=2),
    PRIMARY KEY (article_id)
)
```
