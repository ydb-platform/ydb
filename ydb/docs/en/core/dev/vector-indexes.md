# Vector indexes

[Vector indexes](https://en.wikipedia.org/wiki/Vector_database) are specialized data structures that enable efficient similarity search in high-dimensional spaces. Unlike traditional indexes that optimize exact lookups, vector indexes allow finding the most similar items to a query vector based on mathematical distance or similarity measures.

Data in a {{ ydb-short-name }} table is stored and sorted by a primary key, enabling efficient point lookups and range scans. Vector indexes provide similar efficiency for nearest neighbor searches in vector spaces, which is particularly valuable for working with embeddings and other high-dimensional data representations.

This article describes practical operations with vector indexes. For conceptual information about vector index types and their characteristics, see [Vector indexes](../concepts/vector_indexes.md) in the Concepts section.

## Creating vector indexes {#create}

A vector index can be created with the following YQL commands:
* [`CREATE TABLE`](../yql/reference/syntax/create_table/index.md)
* [`ALTER TABLE`](../yql/reference/syntax/alter_table/index.md)

Example of creating a prefixed vector index with covered columns:

```yql
ALTER TABLE my_table
  ADD INDEX my_index
  GLOBAL USING vector_kmeans_tree
  ON (user, embedding) COVER (data)
  WITH (distance=cosine, type="uint8", dimension=512, levels=2, clusters=128);
```

Key parameters for `vector_kmeans_tree`:
* distance/similarity: Metric function ("cosine", "euclidean", etc.)
* type: Data type ("float", "int8", "uint8")
* dimension: Number of dimensions (<= 16384)
* levels: Tree depth
* clusters: Number of clusters per level (values > 1000 may impact performance)

Since building a vector index requires processing existing data, index creation on populated tables may take significant time. This operation runs in the background, allowing continued table access during construction. The index becomes available automatically when ready.

## Using vector indexes for similarity search {#use}

To perform similarity searches, explicitly specify the index name in the VIEW clause. For prefixed indexes, include prefix column conditions in the WHERE clause:

```yql
DECLARE $query_vector AS List<Uint8>;

SELECT user, data
FROM my_table VIEW my_index
WHERE user = "john_doe"
ORDER BY Knn::CosineSimilarity(embedding, $query_vector) DESC
LIMIT 10;
```

Without the VIEW clause, the query would perform a full table scan with brute-force vector comparison.

## Checking the cost of queries {#cost}

Any query made in a transactional application should be checked in terms of the number of I/O operations it performed in the database and how much CPU was used to run it. You should also make sure these indicators don't continuously grow as the database volume grows. {{ ydb-short-name }} returns statistics required for the analysis after running each query.

If you use the {{ ydb-short-name }} CLI, select the `--stats` option to enable printing statistics after executing the `yql` command. All {{ ydb-short-name }} SDKs also contain structures with statistics returned after running a query. If you make a query in the UI, you'll see a tab with statistics next to the results tab.

{% note warning %}

Vector indexes currently don't support data modification operations. 
Any attempt to modify rows in indexed tables will fail. 
This limitation will be removed in future releases.

{% endnote %}
