# Vector Indexes

[Vector indexes](../concepts/glossary.md#vector-index) are specialized data structures that enable efficient [vector search](../concepts/query_execution/vector_search.md) in multidimensional spaces. Unlike [secondary indexes](../concepts/glossary.md#secondary-index), which optimize searching by equality or range, vector indexes allow similarity searching based on [similarity or distance functions](../yql/reference/udf/list/knn.md#functions).

Data in a {{ ydb-short-name }} table is stored and sorted by the primary key, ensuring efficient searching by exact match and range scanning. Vector indexes provide similar efficiency for nearest neighbor searches in vector spaces.

## Characteristics of Vector Indexes {#characteristics}

Vector indexes in {{ ydb-short-name }} address the nearest neighbor search problem using [similarity or distance functions](../yql/reference/udf/list/knn.md#functions-distance). Distance/similarity function parameters for vector indices:

* `distance` is a distance function ( `cosine` , `euclidean` , `manhattan` ), mutually exclusive with `similarity`.
* `similarity` - similarity function ( `inner_product` , `cosine` ), mutually exclusive with `distance`.

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

### Overlapping clusters {#overlap-clusters}

Vector index in YDB can add each vector to multiple clusters to increase quality of the vector search:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128, overlap_clusters=3);
```

Here, each vector will be added to 3 nearest clusters instead of 1.

Using such an index dramatically increases vector search quality, even with small PRAGMA
[KMeansTreeSearchTopSize](../yql/reference/syntax/select/vector_index.md#kmeanstreesearchtopsize)
values (for example, 3).

## Creating Vector Indexes {#creation}

Vector indexes can be created:

* during table creation using the YQL operator [CREATE TABLE](../yql/reference/syntax/create_table/vector_index.md);
* added to an existing table using the YQL operator [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md).

## Using Vector Indexes {#select}

Queries to vector indexes are executed using the `VIEW` syntax in YQL:

```yql
DECLARE $query_vector AS List<Uint8>;

SELECT user, data
FROM my_table VIEW my_index
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

For filtered indexes, specify the columns in the `WHERE` clause:

```yql
DECLARE $query_vector AS List<Uint8>;

SELECT user, data
FROM my_table VIEW my_index
WHERE user = 'john'
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

For more details on executing `SELECT` queries using vector indexes, see the section [VIEW VECTOR INDEX](../yql/reference/syntax/select/vector_index.md).

{% note info %}

If the `VIEW` expression is not used, the query will perform a full table scan with pairwise comparison of vectors.

It is recommended to check the optimality of the written query using [query statistics](query-plans-optimization.md). In particular, ensure there is no full scan of the main table.

<<<<<<< HEAD
{% endnote %}
=======
```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  COVER (embedding, data)
  WITH (distance=cosine, vector_type="float", vector_dimension=512, levels=2, clusters=128, overlap_clusters=3);
```

## Distance Functions {#distance}

The following [similarity or distance functions](../yql/reference/udf/list/knn.md#functions-distance) are supported:

* `distance=cosine` or `similarity=cosine` — cosine distance, corresponds to `ORDER BY Knn::CosineDistance(...) ASC` or `ORDER BY Knn::CosineSimilarity(...) DESC`.
* `distance=manhattan` — Manhattan distance (L1 metric), corresponds to `ORDER BY Knn::ManhattanDistance(...) ASC`.
* `distance=euclidean` — Euclidean distance (L2 metric), corresponds to `ORDER BY Knn::EuclideanDistance(...) ASC`.
* `similarity=inner_product` — inner product, corresponds to `ORDER BY Knn::InnerProductSimilarity(...) DESC`.

## Full Vector Index Syntax {#syntax}

Creating a vector index:

* During table creation: [CREATE TABLE](../yql/reference/syntax/create_table/vector_index.md).
* Adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md).

Full syntax for queries using a vector index:

* [VIEW VECTOR INDEX](../yql/reference/syntax/select/vector_index.md).

## Search Algorithm

The current implementation offers one type of index: `vector_kmeans_tree`.

### Vector Index Type `vector_kmeans_tree` {#kmeans-tree-type}

The `vector_kmeans_tree` index implements hierarchical data clustering. The structure of the index includes:

1. Hierarchical clustering:

    * the index builds multiple levels of k-means clusters;
    * at each level, vectors are distributed across a predefined number of clusters raised to the power of the level;
    * the first level clusters the entire dataset;
    * subsequent levels recursively cluster the contents of each parent cluster.

2. Search process:

    * search proceeds recursively from the first level to the subsequent ones;
    * during queries, the index analyzes only the most promising clusters;
    * such search space pruning avoids complete enumeration of all vectors.

3. Parameters:

    * `levels`: number of levels in the tree, defining search depth (recommended 1-3);
    * `clusters`: number of clusters in k-means, defining search width (recommended 64-512).
    * `overlap_clusters`: number of leaf-level clusters each vector is added to (recommended 3).

Internally, a vector index consists of index tables named `indexImpl*Table`. In selection queries using the vector index, these tables appear in [query statistics](query-execution-optimization/query-plans-optimization.md). For more on the structure of the vector index, see the dedicated article [{#T}](vector-indexes-kmeans-tree-type.md).

### Overlapping Clusters {#overlap-clusters}

A vector index in YDB can add each vector to multiple clusters to improve search recall and speed:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, vector_type="float", vector_dimension=512, levels=2, clusters=128, overlap_clusters=3);
```

In this example, each vector will be added to 3 nearest clusters instead of 1.

The `overlap_clusters` parameter is recommended for nearly all use cases, especially for vector indexes with `levels > 1`, as it significantly improves search recall even with small [`PRAGMA KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#kmeanstreesearchtopsize) values (for example, 3).

This way, you can reduce the PRAGMA value and significantly speed up the search while maintaining the same recall.

## Partitioning of Index Tables {#partitioning}

The most heavily loaded table in a vector index is `indexImplLevelTable`, the cluster structure table. Every search query reads this table, so load on its partitions may limit query performance.

To improve performance, you can enable auto-partitioning by load:

```yql
ALTER TABLE `my_table/my_index/indexImplLevelTable`
SET AUTO_PARTITIONING_BY_LOAD ENABLED;
```

Or by size:

```yql
ALTER TABLE `my_table/my_index/indexImplLevelTable`
SET AUTO_PARTITIONING_PARTITION_SIZE_MB 100;
```

The same settings can be applied to other index tables (`indexImplPostingTable` and `indexImplPrefixTable`), but the Level table is the most loaded while being small in size, so auto-partitioning settings are most relevant for it.

## Using Index Table Replicas {#replicas}

Another way to speed up search is to use table replicas. To do this:

1. Create a [covering index](#covering) so that only index tables are involved in search queries.
2. Enable replicas on all index tables:

   ```yql
   ALTER TABLE `my_table/my_index/indexImplLevelTable` SET READ_REPLICAS_SETTINGS 'PER_AZ:3';
   ALTER TABLE `my_table/my_index/indexImplPostingTable` SET READ_REPLICAS_SETTINGS 'PER_AZ:3';
   ```

   And, for a filtered index, also:

   ```yql
   ALTER TABLE `my_table/my_index/indexImplPrefixTable` SET READ_REPLICAS_SETTINGS 'PER_AZ:3';
   ```

3. Use the [Stale Read-Only](../recipes/ydb-sdk/tx-control.md#stale-read-only) query mode.
>>>>>>> d0dd62b7f9b (modify menu for optimization plans in 'for developers' menu (#38242))

## Updating Vector Indexes {#update}

When a table with a vector index is updated, its internal structure — a tree of clusters (groups of similar vectors) — is not recalculated. New or modified records are simply assigned to existing clusters.

Over time, this can lead to index degradation, resulting in:

1. Reduced completeness — the index may return fewer relevant results because clusters no longer reflect the actual data distribution.
2. Reduced performance — unbalanced clusters (for example, one cluster containing too many records) can slow down search queries and, in the worst case, lead to full table scans.

The extent of degradation depends on the nature of the updates:

* If the index was built on a representative sample (e.g., a random 50% of the data) and the remaining records are added later, the index structure remains mostly relevant, and degradation is minimal.
* If entire groups of similar vectors were absent from the initial dataset, the clustering may fail to partition the space effectively, leading to a significant drop in result relevance.

A particularly problematic corner case arises when a vector index is created on an empty table. In this scenario, the index consists of a single cluster, and all new records are placed within it. As a result, searches using such an index are equivalent to full table scans.

To prevent degradation:

* Avoid creating a vector index on an empty table.
* If a large volume of new data has been added, [build a new index](../yql/reference/syntax/alter_table/indexes.md) and [atomically replace](../reference/ydb-cli/commands/secondary_index.md#rename) the old index with the updated one.
