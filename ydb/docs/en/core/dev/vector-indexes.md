# Vector Indexes

[Vector indexes](../concepts/glossary.md#vector-index) are a specialized type of [secondary index](../concepts/glossary.md#secondary-index) that enable efficient [vector search](../concepts/query_execution/vector_search.md) in multidimensional spaces. While traditional secondary indexes optimize searching by equality or range, vector indexes allow similarity searching based on [similarity or distance functions](../yql/reference/udf/list/knn.md#functions).

Data in a {{ ydb-short-name }} table is stored and sorted by the primary key, ensuring efficient searching by exact match and range scanning. Vector indexes provide similar efficiency for nearest neighbor searches in vector spaces.

## Types of Vector Indexes {#types}

A vector index can be [global](#global) or [global filtered](#filtered). Indexes of any of these types can also be [covering](#covering) and include a copy of additional column data from the main table.

### Global Vector Index {#global}

A global vector index on the `embedding` column enables fast approximate nearest neighbor search across the entire table:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  COVER (embedding, data)
  WITH (distance=cosine, vector_type="float", vector_dimension=512, overlap_clusters=3);
```

Example search query using this index:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

DECLARE $query_vector AS string;

$query_vector = Knn::ToBinaryStringFloat([1.0, 1.2, 0, ...]);

SELECT user, data
FROM my_table VIEW my_index
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

Note that:

- Both the `embedding` column and the `$query_vector` parameter must be of string type and contain an array of numbers in the simple [binary format](../yql/reference/udf/list/knn.md#functions-convert-format).
- It is more efficient to pass the parameter from the SDK as a string by serializing the numbers on the application side ([examples](../recipes/ydb-sdk/vector-search.md#search-by-vector)). Alternatively, the value can be passed from the SDK as a vector of numbers and converted from a list using `Knn::ToBinaryString*` functions, but this is slower.
- The `COVER (embedding, data)` clause is optional and is used to create a [covering index](#covering). This helps further speed up the search.
- Vector index search is always approximate — its results differ from a full-scan search.
- Increasing the [`PRAGMA KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize) parameter improves search quality (recall) at the cost of speed. The parameter sets the number of index clusters nearest to the query that are scanned. The default value is 4 with overlapping clusters (`overlap_clusters > 1`) and 10 without overlap.
- The `overlap_clusters=3` parameter significantly improves future search quality during indexing by specifying the maximum number of leaf clusters each vector is added to, but increases the index size.
- The `vector_type` and `vector_dimension` parameters can be omitted if the table is not empty — they will be autodetected from existing rows.

### Filtered Vector Index {#filtered}

A filtered vector index enables searching for nearest neighbors within each category defined by unique values of additional columns.

To create such an index, specify multiple index columns. The last column must be the vector column; the others (category columns) can be of any type:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding)
  COVER (embedding, data)
  WITH (distance=cosine, vector_type="float", vector_dimension=512);
```

Search queries using this filtered index can include conditions on the `user` column:

```yql
PRAGMA ydb.KMeansTreeSearchTopSize = "10";

DECLARE $query_vector AS string;

$query_vector = Knn::ToBinaryStringFloat([1.0, 1.2, 0, ...]);

SELECT user, data
FROM my_table VIEW my_index
WHERE user = 'john'
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

You can search several categories using `IN` or `OR`. With multiple filtering columns, a condition on a leading part of the prefix is also supported. See [filtering with a prefixed vector index](../yql/reference/syntax/select/vector_index.md#filtering).

Indexing and search parameters work the same as for a global index. Because different filtering-column values often hold very different numbers of vectors, a filtered index can additionally use [adaptive clusters](vector-indexes-kmeans-tree-type.md#adaptive-clusters) to pick the number of clusters for each value automatically.

### Covering Vector Index {#covering}

A covering vector index stores a copy of additional column data to avoid reading from the main table and further speed up the search.

Note that by default the index does not contain a copy of the vector column (in the example — `embedding`), so if it is not explicitly added to the list of covered columns, reading from the main table cannot be avoided, since vectors are always used for exact result sorting in the final search step.

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  COVER (embedding, data)
  WITH (distance=cosine);
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
    * `overlap_clusters`: maximum number of leaf-level clusters each vector is added to (recommended 3).

Internally, a vector index consists of index tables named `indexImpl*Table`. In selection queries using the vector index, these tables appear in [query statistics](optimization/plans.md). For more on the structure of the vector index, see the dedicated article [{#T}](vector-indexes-kmeans-tree-type.md).

### Overlapping Clusters {#overlap-clusters}

A vector index in YDB can add each vector to multiple clusters to improve search recall and speed:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, overlap_clusters=3);
```

In this example, each vector will be added to up to 3 nearest leaf clusters instead of 1.

The `overlap_clusters` parameter is recommended for nearly all use cases, especially for vector indexes with `levels > 1`, as it significantly improves search recall even with small [`PRAGMA KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize) values (for example, 3).

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

## Data requirements and limitations {#limitations}

The vector column stores serialized vectors as `String`. Use the [Knn conversion functions](../yql/reference/udf/list/knn.md#functions-convert) to produce this representation. Vectors must match the index type and dimension. During index construction, a nonempty vector with an incompatible dimension causes the build to fail with `Vector dimension mismatch`; `NULL` and empty embeddings are skipped.

Tables with vector indexes currently do not support [TTL](../concepts/ttl.md). Creating an index on a table with TTL enabled, or enabling TTL on a table with a vector index, is rejected.

`BulkUpsert` does not support tables with synchronous vector indexes. Load data with `BulkUpsert` before creating the index, or use YQL `INSERT` and `UPSERT` to update an indexed table.

## Updating Vector Indexes {#update}

After the index is built, `INSERT`, `UPSERT`, `UPDATE`, and `DELETE` update it synchronously with the main table. A vector search in the same transaction sees earlier writes in that transaction. The following limitations still apply:

### Clusters are not recalculated during update

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
* If a large volume of new data has been added, [rebuild the index](#rebuild) when search quality or performance has degraded.

To decide when to rebuild:

1. Choose a representative set of query vectors. Measure search recall by comparing indexed results with exact results from a full scan of the same table. The [vector workload command](../reference/ydb-cli/workload-vector.md#run-select) demonstrates this with `--recall`.
2. Record search latency for the same queries. Repeat the measurements with the same distance function and search settings, including [`KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize).
3. Rebuild if recall falls or latency rises consistently after the data distribution changes. Row growth alone is a reason to measure, not a fixed rebuild threshold.

### Update inconsistency during index build {#build-consistency}

Vector indexes do not support consistent updates during build. That is, a vector index is not updated when data in the main table is modified until the index build is finished.

This means that if you want a vector index to remain 100% consistent, you have to pause table updates while it is being built.

Updates are not blocked automatically because vector index search is approximate by nature, and in many cases temporary inconsistency during the build is acceptable.

This temporary limitation is planned to be removed in a future {{ ydb-short-name }} release.

## Rebuilding a Vector Index {#rebuild}

Rebuilding creates a new cluster tree and redistributes the table's vectors across it. Use [`ALTER TABLE ... REBUILD INDEX`](../yql/reference/syntax/alter_table/indexes.md#rebuild-index) when changes in the data distribution reduce search recall or performance:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`;
```

The command preserves the index name, indexed and covered columns, and vector index settings. To adjust the tree for a changed dataset size, explicitly set `clusters` and `levels`, which control the number of clusters and tree levels:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`
WITH (clusters = 128, levels = 2);
```

The existing index continues to serve queries and receive table updates during the build. Once the replacement is ready, {{ ydb-short-name }} atomically replaces the old index. Applications continue to use the same index name. The operation temporarily requires storage for both index versions and resources to build the replacement.

To limit the number of parallel partition handlers during the rebuild, set the [`parallel` parameter](../yql/reference/syntax/alter_table/indexes.md#rebuild-index). For example, to run no more than eight handlers at a time:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`
WITH (parallel = 8);
```

The replacement is built from a snapshot, so the [consistency limitation during index building](#build-consistency) also applies to rebuilding. If you need a fully consistent index:

1. Stop all application writers and ingestion jobs for the table, and wait for in-flight writes to finish. {{ ydb-short-name }} does not pause writes automatically.
2. Start the rebuild. Find its ID with [`ydb operation list buildindex`](../reference/ydb-cli/operation-list.md), then check it with [`ydb operation get`](../reference/ydb-cli/operation-get.md).
3. Resume writes when the operation reports `ready: true` and `status: SUCCESS`.

Queries remain available during rebuilding, but may require a [retry](../recipes/ydb-sdk/retry.md) when the index is replaced.

## Recipes for Working with Vector Indexes {#vector-index-recipes}

To get started with vector indexes, you can use the following recipes:

* [YDB CLI & YQL](../recipes/vector-search)
