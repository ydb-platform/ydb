# Vector indexes

{{ ydb-short-name }} supports specialized _vector indexes_ to efficiently find the top k rows with vector values closest to a query vector. Unlike secondary indexes that optimize equality or range queries, vector indexes enable similarity search based on distance or similarity functions.

## Vector index characteristics {#characteristics}

Vector indexes in {{ ydb-short-name }}:

* Solve nearest neighbor search problems using similarity or distance functions
* Support multiple distance/similarity functions: "inner_product", "cosine" similarity and "cosine", "euclidean", "manhattan" distance
* Currently implement a single index type: `vector_kmeans_tree`

### Vector index `vector_kmeans_tree` type {#vector-kmeans-tree-type}

The `vector_kmeans_tree` index implements a hierarchical clustering structure. Its organization includes:

1. Hierarchical clustering:
  - The index builds multiple levels of k-means clusters
  - At each level, vectors are partitioned into specified number of clusters in power of level
  - First level clusters the entire dataset
  - Subsequent levels recursively cluster each parent cluster's contents

2. Search process:
  - During queries, the index examines only the most promising clusters
  - This search space pruning avoids exhaustive search through all vectors

3. Parameters:
  - `levels`: The number of tree levels (typically 1-3). Controls search depth
  - `clusters`: The number of clusters on each level (typically 64-512). Determines search breadth at each level 

## Vector index types {#types}

### Basic vector index {#basic}

The simplest form that indexes vectors without additional filtering capabilities. Example creation:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, type="uint8", dimension=512, levels=2, clusters=128);
```

### Vector index with covered columns {#covering}

Includes additional columns to avoid reading from the main table during queries:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding) COVER (data)
  WITH (distance=cosine, type="uint8", dimension=512, levels=2, clusters=128);
```

### Prefixed vector index {#prefixed}

Allows filtering by prefix columns before performing vector search:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding)
  WITH (distance=cosine, type="uint8", dimension=512, levels=2, clusters=128);
```

### Prefixed vector index with covered columns {#prefixed-covering}

Combines prefix filtering with covered columns for optimal performance:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding) COVER (data)
  WITH (distance=cosine, type="uint8", dimension=512, levels=2, clusters=128);
```

## Creating vector indexes {#creation}

Vector indexes can be created:

* When creating a table with the YQL CREATE TABLE statement
* 

* When creating a table with the YQL [CREATE TABLE](../../yql/reference/syntax/create_table/vector_index.md) statement
* Added to an existing table with the YQL [ALTER TABLE](../../yql/reference/syntax/alter_table/indexes.md) statement

For more information about vector index parameters, see [CREATE TABLE](../../yql/reference/syntax/create_table/vector_index.md).

## Using vector indexes {#usage}

Query vector indexes using the VIEW syntax in YQL. For prefixed indexes, include the prefix columns in the WHERE clause:

```yql
SELECT user, data
FROM my_table VIEW my_index
WHERE user = "..."
ORDER BY Knn::CosineSimilarity(embedding, ...) DESC
LIMIT 10;
```

## Typical use cases {#usecases}

Vector indexes are particularly useful for:

* Recommendation systems (finding similar items/users)
* Semantic search (matching text embeddings)
* Image similarity search
* Anomaly detection (finding outliers)
* Classification systems (finding nearest labeled examples)

## Limitations {#limitations}

Currently not supported:
* modifying rows in indexed tables;
* bit vector type.
