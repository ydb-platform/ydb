# Vector Index — Quick Start

This article will help you quickly get started with vector indexes in {{ ydb-short-name }} using a simple model example.

The article will cover the following steps for working with vector indexes:

* [creating a table with vectors](#step1);
* [populating the table with data](#step2);
* [building a vector index](#step3);
* [performing vector search without an index](#step4);
* [performing vector search with an index](#step5).

## Step 1. Creating a table with vectors {#step1}

First, you need to create a table in {{ ydb-short-name }} that will store vectors. This can be done using an SQL query:

```yql
CREATE TABLE Vectors (
  id Uint64,
  embedding String,
  PRIMARY KEY (id)
);
```

This `Vectors` table has two columns:

- `id` — a unique identifier for each vector
- `embedding` — a vector of real numbers [packed into a string](../../yql/reference/udf/list/knn.md#functions-convert)

## Step 2. Populating the table with data {#step2}

After creating the table, you should add vectors to it using an `UPSERT INTO` query:

```yql
UPSERT INTO Vectors(id, embedding)
VALUES
    (1, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.f]), "FloatVector")),
    (2, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.25f]), "FloatVector")),
    (3, Untag(Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 1.5f]), "FloatVector")),
    (4, Untag(Knn::ToBinaryStringFloat([-1.f, -1.f, -1.f, -1.f, -1.f]), "FloatVector")),
    (5, Untag(Knn::ToBinaryStringFloat([-2.f, -2.f, -2.f, -2.f, -4.f]), "FloatVector")),
    (6, Untag(Knn::ToBinaryStringFloat([-3.f, -3.f, -3.f, -3.f, -6.f]), "FloatVector"));
```

For a description of the `Knn::ToBinaryStringFloat` function, see [{#T}](../../yql/reference/udf/list/knn.md).

## Step 3. Building a vector index {#step3}

To create a vector index `EmbeddingIndex` on the `Vectors` table, use the following command:

```yql
ALTER TABLE Vectors
ADD INDEX EmbeddingIndex
GLOBAL USING vector_kmeans_tree
ON (embedding)
WITH (
  distance=cosine,
  vector_type="float",
  vector_dimension=5,
  levels=1,
  clusters=2)
```

This command creates an index of the `vector_kmeans_tree` type. For more information about indexes of this type, see [{#T}](../../dev/vector-indexes.md#kmeans-tree-type). In this model example, the parameter `clusters=2` is specified (splitting the set of vectors when building the index into two clusters at each level); for real data, values in the range from 64 to 512 are recommended.

For general information about vector indexes, their creation parameters, and current limitations, see the section [{#T}](../../dev/vector-indexes.md).

## Step 4. Searching the table without using a vector index {#step4}

At this step, an exact search for the 3 nearest neighbors for the given vector `[1.f, 1.f, 1.f, 1.f, 4.f]` is performed **without** using an index.

First, the target vector is encoded into a binary representation using [`Knn::ToBinaryStringFloat`](../../yql/reference/udf/list/knn#functions-convert).

Then, the cosine distance from the `embedding` of each row to the target vector is calculated.

Records are sorted by increasing distance, and the first three (`$K`) records are selected, which are the nearest ones.

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 4.f]);

SELECT id, Knn::CosineDistance(embedding, $TargetEmbedding) As CosineDistance
FROM Vectors
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

Query execution result:

```bash
id CosineDistance
3  0.1055728197
2  0.1467181444
1  0.1999999881
```

For detailed information about exact vector search without using vector indexes, see [{#T}](../../yql/reference/udf/list/knn.md).

## Step 5. Searching the table using a vector index {#step5}

To search for the 3 nearest neighbors of the vector `[1.f, 1.f, 1.f, 1.f, 4.f]` using the `EmbeddingIndex` index that was created in [step 3](#step3), execute the following query:

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.f, 1.f, 1.f, 1.f, 4.f]);

SELECT id, Knn::CosineDistance(embedding, $TargetEmbedding) As CosineDistance
FROM Vectors VIEW EmbeddingIndex
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

Note that in this query, after the table name, it is specified that record selection should be performed using the vector index: `FROM Vectors VIEW EmbeddingIndex`.

Query execution result:

```bash
id CosineDistance
3  0.1055728197
2  0.1467181444
1  0.1999999881
```

Thanks to using the index, searching for the nearest vectors happens significantly faster on large datasets.

## Conclusion

This article provides a simple example of working with vector indexes: creating a table with vectors, populating the table with vectors, building a vector index for such a table, and searching for vectors in the table using a vector index or without it.

In the case of a small table, as in this model example, it's impossible to see the difference in query performance. These examples are intended to illustrate the syntax when working with vector indexes.<!-- For a more realistic example with a larger data volume, see [{#T}](vector-index-with-prepared-dataset.md).-->

For more information about vector indexes, see [{#T}](../../dev/vector-indexes.md).
