# Vector index

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

{% note warning %}

It makes no sense to create an empty table with a vector index, because for now we don't allow mutations in tables with vector indexes.

You should use `ALTER TABLE ... ADD INDEX` to add a vector index to an existing table.

{% endnote %}

The INDEX construct is used to define a {% if concept_vector_index %}[vector index]({{ concept_vector_index }}){% else %}vector index{% endif %} in a [row-oriented](../../../../concepts/datamodel/table.md#row-oriented-tables) table:

```yql
CREATE TABLE table_name (
    ...
    INDEX <index_name> GLOBAL [SYNC] USING <index_type> ON ( <index_columns> ) COVER ( <cover_columns> ) WITH ( <index_parameters> ),
    ...
)
```

Where:

* **Index_name** is the unique name of the index to be used to access data.
* **SYNC** indicates synchronous data writes to the index. If not specified, synchronous.
* **Index_type** is the index type. Only `vector_kmeans_tree` is supported now.
* **Index_columns** is a list of comma-separated column names in the created table to be used for a search in the index. The last column in the list is used as embedding, the other columns are used as prefix columns.
* **Cover_columns** is a list of comma-separated column names in the created table, which will be stored in the index in addition to the search columns, making it possible to fetch additional data without accessing the table for it.
* **Index_parameters** is a list of comma-separated key-value parameters:
    * parameters for any vector **index_type**:
        * `dimension` is a number of dimension in the indexed embedding (<= 16384)
        * `type` is a type of value in the indexed embedding, can be `float`, `uint8`, `int8`, `bit`
        * `distance` is a type of the distance function which will be used for this index. Valid values: `cosine`, `manhattan`, `euclidean`.
        * `similarity` is a type of the similarity function which will be used for this index. Valid values: `inner_product`, `cosine`.
    * parameters specific to `vector_kmeans_tree`:
        * `clusters` is a `k` in each kmeans used for tree (values > 1000 can affect performance)
        * `levels` is a level count in the tree


{% note warning %}

The `distance` and `similarity` parameters can not be specified together.

{% endnote %}


{% note warning %}

The `type=bit` vector index is not supported yet.

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
    WITH (dimension=512, type="float", distance="cosine", clusters=128, levels=2),
    PRIMARY KEY (article_id)
)
```
