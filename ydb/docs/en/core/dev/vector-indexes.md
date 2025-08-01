# Vector Indexes

{% include [limitations](../_includes/vector_index_limitations.md) %}

[Vector indexes](../concepts/glossary.md#vector-index) are specialized data structures that enable efficient [vector search](../concepts/vector_search.md) in multidimensional spaces. Unlike [secondary indexes](../concepts/glossary.md#secondary-index), which optimize searching by equality or range, vector indexes allow similarity searching based on [similarity or distance functions](../yql/reference/udf/list/knn.md#functions).

Data in a {{ ydb-short-name }} table is stored and sorted by the primary key, ensuring efficient searching by exact match and range scanning. Vector indexes provide similar efficiency for nearest neighbor searches in vector spaces.

## Characteristics of Vector Indexes {#characteristics}

Vector indexes in {{ ydb-short-name }} address the nearest neighbor search problem using [similarity or distance functions](../yql/reference/udf/list/knn.md#functions). Several distance/similarity functions are supported: "inner_product", "cosine" (similarity) and "cosine", "euclidean", "manhattan" (distance).

The current implementation offers one type of index: `vector_kmeans_tree`.

## Vector Index Type `vector_kmeans_tree` {#kmeans-tree-type}

The `vector_kmeans_tree` index implements hierarchical data clustering. The structure of the index includes:

1. Hierarchical clustering:

    * the index builds multiple levels of k-means clusters
    * at each level, vectors are distributed across a predefined number of clusters raised to the power of the level
    * the first level clusters the entire dataset
    * subsequent levels recursively cluster the contents of each parent cluster

2. Search process:

    * search proceeds recursively from the first level to the subsequent ones
    * during queries, the index analyzes only the most promising clusters
    * such search space pruning avoids complete enumeration of all vectors

3. Parameters:

    * `levels`: number of levels in the tree, defining search depth (recommended 1-3)
    * `clusters`: number of clusters in k-means, defining search width (recommended 64-512)

Internally, a vector index consists of hidden index tables named `indexImpl*Table`. In [selection queries](#select) using the vector index, the index tables will appear in [query statistics](query-plans-optimization.md).

## Types of Vector Indexes {#types}

A vector index can be **covering**, meaning it includes additional columns to enable reading from the index without accessing the main table.

Alternatively, it can be **filtered**, allowing for additional columns to be used for quick filtering during reading.

Below are examples of creating vector indexes of different types.

### Basic Vector Index {#basic}

Global vector index on the `embedding` column:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Vector Index with Covering Columns {#covering}

A covering vector index, including an additional column `data` to avoid reading from the main table during a search:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding) COVER (data)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Filtered Vector Index {#filtered}

A filtered vector index, allowing filtering by the column `user` during vector search:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Filtered Vector Index with Covering Columns {#filtered-covering}

A filtered vector index with covering columns:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding) COVER (data)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

## Creating Vector Indexes {#creation}

Vector indexes can be created:

* during table creation using the YQL operator [CREATE TABLE](../yql/reference/syntax/create_table/vector_index.md);
* added to an existing table using the YQL operator [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md).

## Using Vector Indexes {#select}

Queries to vector indexes are executed using the `VIEW` syntax in YQL. For filtered indexes, specify the columns in the `WHERE` clause:

```yql
DECLARE $query_vector AS List<Uint8>;

SELECT user, data
FROM my_table VIEW my_index
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

For more details on executing `SELECT` queries using vector indexes, see the section [VIEW VECTOR INDEX](../yql/reference/syntax/select/vector_index.md).

{% note info %}

If the `VIEW` expression is not used, the query will perform a full table scan with pairwise comparison of vectors.

It is recommended to check the optimality of the written query using [query statistics](query-plans-optimization.md). In particular, ensure there is no full scan of the main table.

{% endnote %}

