# Vector indexes

{{ ydb-short-name }} supports specialized _vector indexes_ to efficiently find the top k rows with vector values closest to a query vector. Unlike secondary indexes that optimize equality or range queries, vector indexes enable similarity search based on distance or similarity functions.

## Vector index characteristics {#characteristics}

Vector indexes in {{ ydb-short-name }}:

* Solve nearest neighbor search problems using similarity or distance functions
* Currently don't support modifying rows in tables with vector indexes (planned for future releases)
* Support multiple distance/similarity functions: "inner_product", "cosine" similarity and "cosine", "euclidean", "manhattan" distance
* Currently implement a single index type: `vector_kmeans_tree`

## Vector index types {#types}

### Basic vector index {#basic}

The simplest form that indexes vectors without additional filtering capabilities. Example creation:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Vector index with covered columns {#covering}

Includes additional columns to avoid reading from the main table during queries:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (embedding) COVER (data)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Prefixed vector index {#prefixed}

Allows filtering by prefix columns before performing vector search:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

### Prefixed vector index with covered columns {#prefixed-covering}

Combines prefix filtering with covered columns for optimal performance:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding) COVER (data)
  WITH (distance=cosine, vector_type="uint8", vector_dimension=512, levels=2, clusters=128);
```

## Creating vector indexes {#creation}

Vector indexes can be created:

* When creating a table with the YQL CREATE TABLE statement
* Added to an existing table with the YQL ALTER TABLE statement

Required parameters for vector_kmeans_tree:
* distance or similarity: The function to use (e.g., "cosine")
* vector_type: Data type of vector elements ("float", "int8", "uint8")
* vector_dimension: Dimensionality of vectors (<= 16384)
* levels: Number of tree levels
* clusters: Number of clusters per level (values > 1000 may impact performance)

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

Vector indexes currently don't support modifying rows in indexed tables

Bit vector type currently not supported
