# Vector Indexes

[Vector indexes](../concepts/glossary.md#vector-index) are a specialized type of [secondary index](../concepts/glossary.md#secondary-index) that enable efficient [vector search](../concepts/query_execution/vector_search.md) in multidimensional spaces. Unlike traditional secondary indexes optimized for equality or range search, vector indexes allow approximate search based on [similarity or distance functions](../yql/reference/udf/list/knn.md#functions).

Data in a {{ ydb-short-name }} table is stored and sorted by the primary key, which ensures efficient exact-match search and range scanning. Vector indexes provide similar efficiency for nearest neighbor search in vector spaces.

## Types of Vector Indexes {#types}

A vector index can be [global](#global) or [global with filtering](#filtered). Also, any of these index types can be [covering](#covering) and include a copy of additional column data from the main table.

### Global Vector Index {#global}

A global vector index on the `embedding` column allows fast approximate nearest neighbor search across the entire table:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  COVER (embedding, data)
  WITH (distance=cosine, vector_type="float", vector_dimension=512, overlap_clusters=3);
```

Example search query to such an index:

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

- Both the vector column `embedding` and the `$query_vector` parameter must be of string type and contain an array of numbers in the simple [binary format](../yql/reference/udf/list/knn.md#functions-convert-format).
- It is more efficient to pass the parameter from the SDK as a string, serializing the numbers on the application side ([examples](../recipes/ydb-sdk/vector-search.md#search-by-vector)). Alternatively, the value can be passed from the SDK as a vector of numbers and converted from a list using `Knn::ToBinaryString*` functions, but this is slower.
- The `COVER (embedding, data)` clause is optional and is intended for creating a [covering index](#covering). This helps further speed up the search.
- Vector index search is always approximate — its results differ from exhaustive search.
- Increasing the [`PRAGMA KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize) parameter improves search quality (recall) but reduces its speed. The parameter sets the number of index clusters nearest to the query that are scanned. The default value is 4 with overlapping clusters (`overlap_clusters > 1`) and 10 without overlap.
- The `overlap_clusters=3` parameter significantly improves future search quality during indexing by specifying the maximum number of leaf clusters each vector is added to, but increases the index size.
- The `vector_type` and `vector_dimension` parameters can be omitted if the table is not empty — they will be automatically determined from the row contents.

### Filtered Vector Index {#filtered}

A filtered vector index allows searching for nearest neighbors within each category defined by a unique value of additional columns.

To create such an index, specify multiple index columns. The last column must be the vector column, the others (category columns) can be of any type:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding)
  COVER (embedding, data)
  WITH (distance=cosine);
```

Search queries to such a filtered index can include conditions on the `user` column:

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

Use `IN` or `OR` to search across multiple categories. If there are multiple filtering columns, a condition on the leading part of the prefix is also supported. See [filtering by vector index prefix](../yql/reference/syntax/select/vector_index.md#filtering).

Indexing and search parameters work here similarly to a global index. Since different values of filtering columns often contain very different numbers of vectors, a filtered index can additionally use [adaptive number of clusters](vector-indexes-kmeans-tree-type.md#adaptive-clusters) to select the number of clusters for each value automatically.

### Covering Vector Index {#covering}

A covering vector index contains a copy of additional column data to avoid reading from the main table and further speed up the search.

Note that by default the index does not contain a copy of the vector column (in the example — embedding), so if it is not explicitly added to the list of covered columns, reading from the main table cannot be avoided, since vectors are always used for exact sorting of results at the final search stage.

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

* `distance=cosine` or `similarity=cosine` — cosine distance, corresponds to sorting `ORDER BY Knn::CosineDistance(...) ASC` or `ORDER BY Knn::CosineSimilarity(...) DESC`.
* `distance=manhattan` — Manhattan distance (L1 metric), corresponds to `ORDER BY Knn::ManhattanDistance(...) ASC`.
* `distance=euclidean` — Euclidean distance (L2 metric), corresponds to `ORDER BY Knn::EuclideanDistance(...) ASC`.
* `similarity=inner_product` — inner product, corresponds to `ORDER BY Knn::InnerProductSimilarity(...) DESC`.

## Full Vector Index Syntax {#syntax}

Creating a vector index:

* When creating a table: [CREATE TABLE](../yql/reference/syntax/create_table/vector_index.md).
* Adding to an existing table: [ALTER TABLE](../yql/reference/syntax/alter_table/indexes.md).

Full syntax for queries to a vector index:

* [VIEW VECTOR INDEX](../yql/reference/syntax/select/vector_index.md).

## Search Algorithm

The current implementation offers one index type: `vector_kmeans_tree`.

### Vector Index Type `vector_kmeans_tree` {#kmeans-tree-type}

The `vector_kmeans_tree` index implements hierarchical data clustering. The index structure includes:

1. Hierarchical clustering:

    * the index builds several levels of k-means clusters;
    * at each level, vectors are distributed across a predefined number of clusters raised to the power of the level;
    * the first level clusters the entire dataset;
    * subsequent levels recursively cluster the contents of each parent cluster.

2. Search process:

    * search proceeds recursively from the first level to subsequent ones;
    * when executing queries, the index analyzes only the most promising clusters;
    * such pruning of the search space avoids exhaustive enumeration of all vectors.

3. Parameters:

    * `levels`: number of levels in the tree, defines search depth (recommended 1-3);
    * `clusters`: number of clusters in k-means, defining search width (recommended 64-512).
    * `overlap_clusters`: maximum number of lower-level clusters each vector is added to (recommended 3).

Internally, a vector index consists of hidden index tables of the form `indexImpl*Table`. In [selection queries](../yql/reference/syntax/select/vector_index.md) using a vector index, these tables appear in [query statistics](./optimization/plans.md#analyze-cli). For more details on the vector index structure, see the dedicated article [{#T}](vector-indexes-kmeans-tree-type.md).

### Overlapping Clusters {#overlap-clusters}

A vector index in YDB can add each vector to multiple clusters to improve search recall and speed:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, overlap_clusters=3);
```

In this example, each vector will be added not to 1, but to a maximum of 3 nearest leaf clusters.

The `overlap_clusters` parameter is recommended for almost all use cases, especially for vector indexes with `levels > 1`, as it significantly improves search recall even with small PRAGMA [KMeansTreeSearchTopSize](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize) values (for example, 3).

This way, you can reduce the PRAGMA parameter and significantly speed up the search while maintaining the same recall.

## Partitioning of Index Tables {#partitioning}

The main loaded table of a vector index is `indexImplLevelTable`, the cluster structure table. Any index search query reads this table, so the load on its partitions can limit query performance.

To improve performance, you can enable auto-partitioning by load for it:

```yql
ALTER TABLE `my_table/my_index/indexImplLevelTable`
SET AUTO_PARTITIONING_BY_LOAD ENABLED;
```

Or by size:

```yql
ALTER TABLE `my_table/my_index/indexImplLevelTable`
SET AUTO_PARTITIONING_PARTITION_SIZE_MB 100;
```

Similar settings can be applied to other index tables (`indexImplPostingTable` and `indexImplPrefixTable`), but the Level table is the most loaded while being small in size, so auto-partitioning settings are most relevant for it.

## Using Replicas of Index Tables {#replicas}

Another way to speed up search is to use table replicas. To do this:

1. Create a [covering index](#covering) so that only index tables are involved in the search query.
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

## Data Requirements and Limitations {#limitations}

The vector column stores serialized vectors in the `String` type. To obtain this representation, use the [Knn conversion functions](../yql/reference/udf/list/knn.md#functions-convert). The type and dimension of vectors must match the index. If a non-empty vector of incompatible dimension is encountered during construction, the build fails with the error `Vector dimension mismatch`; `NULL` values and empty embeddings are skipped.

Tables with vector indexes currently do not support [TTL](../concepts/ttl.md). Creating an index on a table with TTL enabled, and enabling TTL on a table with a vector index, both fail with an error.

`BulkUpsert` does not support tables with synchronous vector indexes. Load data via `BulkUpsert` before creating the index, or use YQL `INSERT` and `UPSERT` to update a table with an index.

## Updating Vector Indexes {#update}

After index construction completes, `INSERT`, `UPSERT`, `UPDATE`, and `DELETE` operations update it synchronously with the main table. Vector search in the same transaction sees writes performed earlier. The following limitations still apply:

### Clusters are not recalculated during update

When a table with a vector index is updated, its internal structure — a tree of clusters (groups of similar vectors) — is not rebuilt. New or modified records are only distributed across existing clusters.

Over time, this can lead to index degradation, which manifests in two ways:

* Reduced recall — the index may return fewer relevant results, as clusters no longer reflect the true data structure;
* Reduced performance — if clusters become unbalanced (for example, one cluster contains too many records), search slows down.

The degree of degradation depends on the nature of updates:

* If the index was built on a representative sample (for example, random 50% of the data), and the remaining records were added later, the structure remains relevant, and degradation is minimal;
* If entire groups of similar vectors were initially absent, clusters may partition the space incorrectly, and search quality may drop significantly.

An extreme case is an index created on an empty table: in this case, it contains only one cluster, and all new records fall into it. Search using such an index is equivalent to a full table scan.

To avoid degradation:

* Do not create a vector index on an empty table;
* If a lot of new data has accumulated in the table, [rebuild the index](#rebuild) when search quality or speed has decreased.

To determine when rebuilding is required:

1. Select a representative set of query vectors. Estimate recall by comparing index search results with exact results from a full scan of the same table. An example of such a comparison with the `--recall` parameter is available in the [vector workload command](../reference/ydb-cli/workload-vector.md#run-select).
2. Measure search time for the same queries. Repeat measurements with the same distance function and search settings, including [`KMeansTreeSearchTopSize`](../yql/reference/syntax/select/vector_index.md#KMeansTreeSearchTopSize).
3. Rebuild the index if, after the data distribution changes, recall has consistently decreased or search time has increased. Row growth itself is a reason for measurement, not a fixed rebuild threshold.

### Update inconsistency during build {#build-consistency}

Vector indexes do not support consistent updates during build. That is, the vector index is not updated if data in the main table changes before the index build completes.

This means that if you want the vector index to remain 100% consistent, you must pause data updates in the table during its build.

Table updates are not blocked automatically, since vector index search is always approximate and therefore the lack of consistency during build is often not a problem.

This temporary limitation is planned to be removed in one of the upcoming {{ ydb-short-name }} versions.

## Rebuilding a Vector Index {#rebuild}

During rebuild, a new cluster tree is created, over which the table's vectors are redistributed. Use [`ALTER TABLE ... REBUILD INDEX`](../yql/reference/syntax/alter_table/indexes.md#rebuild-index) if changes in the data distribution have led to a decrease in search recall or speed:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`;
```

The command preserves the index name, the set of indexed and covered columns, and vector index settings. To adapt the tree to the changed data volume, explicitly set `clusters` and `levels`, which determine the number of clusters and tree levels:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`
WITH (clusters = 128, levels = 2);
```

During the build, the existing index continues to serve queries and receive table updates. When the new version is ready, {{ ydb-short-name }} atomically replaces the old index. Applications continue to use the previous index name. During the operation, storage space for both index versions and resources for building the new version are required.

To limit the number of parallel partition handlers during rebuild, set the [`parallel` parameter](../yql/reference/syntax/alter_table/indexes.md#rebuild-index). For example, to run no more than eight handlers simultaneously:

```yql
ALTER TABLE `my_table` REBUILD INDEX `my_index`
WITH (parallel = 8);
```

The new version is built from a data snapshot, so the [consistency limitation during build](#build-consistency) also applies to rebuilding. If a fully consistent index is needed:

1. Stop all applications and ingestion processes writing data to the table, and wait for in-flight write operations to complete. {{ ydb-short-name }} does not pause writes automatically.
2. Start the rebuild. Find the operation ID with the [`ydb operation list buildindex`](../reference/ydb-cli/operation-list.md) command and check its status with the [`ydb operation get`](../reference/ydb-cli/operation-get.md) command.
3. Resume writes when the operation returns `ready: true` and `status: SUCCESS`.

Queries remain available during the rebuild, but a [retry](../recipes/ydb-sdk/retry.md) may be required at the moment the index is replaced.

## Recipes for Working with Vector Indexes {#vector-index-recipes}

To get started with a vector index, you can use the following recipes:

* [YDB CLI & YQL](../recipes/vector-search)
* [YDB SDK: Python, C++](../recipes/ydb-sdk/vector-search.md)
