# INDEX

{% include [not_allow_for_olap](../../../../_includes/not_allow_for_olap_note.md) %}

{% note warning %}

Creating empty table with vector index is quite useless, because for now we don't allow mutation on table with vector index.

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
* **Index_type** is the type of index, only `vector_kmeans_tree` possible now
* **Index_columns** is a list of comma-separated names of columns in the created table to be used for a search in the index, the last column in list used as embedding, the other columns used as prefix columns.
* **Cover_columns** is a list of comma-separated names of columns in the created table, which will be stored in the index in addition to the search columns, making it possible to fetch additional data without accessing the table for it.
* **Index_parameters** is a list of comma-separated key-value parameters:
  * parameters for any vector **index_type**
  * `dimension` -- is a number of dimension in the indexed embedding
  * `type` -- is a type of value in the indexed embedding, can be `float`, `uint8`, `int8`, `bit`
  * `distance` -- is a type of distance function which will be used for this index, can be `cosine`, `manhattan`, `euclidean`
  * `similarity`-- is type of similarity function which will be used for this index, can be `inner_product`,euclidean `cosine`
  * parameters specific to `vector_kmeans_tree`
  * `clusters` -- is a `k` in each kmeans used for tree
  * `levels` -- is a count of levels in the tree


{% note warning %}

`distance` and `similarity` parameters can not be specified together.

{% endnote %}


{% note warning %}

`type=bit` vector index is not supported yet

{% endnote %}

## Example

```yql
CREATE TABLE user_articles (
    atricle_id Uint64,
    user String,
    title String,
    text String,
    embedding String,
    INDEX emb_cosine_idx GLOBAL SYNC USING vector_kmeans_tree ON (user, embedding) COVER (dimension=)
    PRIMARY KEY (article_id)
)
```
