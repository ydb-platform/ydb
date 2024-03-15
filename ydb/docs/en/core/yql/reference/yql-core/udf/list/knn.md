# KNN
## Introduction

[Nearest Neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) is an optimization task that consists of finding the closest point in a given dataset to a given query point. Closeness can be defined in terms of distance or similarity metrics.
A generalization of the NN problem is the [k-NN problem](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), where it's required to find the `k` nearest points to the query point. This can be useful in various applications such as image classification, recommendation systems, etc.
https://en.wikipedia.org/wiki/Nearest_neighbor_search

The solution of the problem is divided into two major subclasses of methods: exact and approximate. In this document, we will talk about the exact brute force method.

The method is based on calculating the distance from the query point to every other point in the database. This algorithm, also known as the naive approach, has a complexity of `O(dn)`, where `n` is the number of points in the dataset, and `d` is its dimension.

The advantage of the method is that there is no need for additional data structures, such as specialized vector indexes.
The disadvantage is the need for a complete data search. But this disadvantage is insignificant in cases where data has been pre-filtered, for example, by user ID.

Example:

```sql
SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(Knn::FromBinaryString(embedding), $TargetEmbedding)
LIMIT 10
```

## Data type

In mathematics, a vector of real numbers is used to store points.
In YDB, for calculations the `List<Float>` data type is used, and data will be stored in the string data type `String`.

## Functions

Vector functions are implemented as user-defined functions (UDF) in the `Knn` module.

### Distance and similarity functions

The distance and similarity functions take two lists of real numbers as input and return the distance/similarity between them.
Distance functions return small values for close vectors, similarity functions return large values for close vectors. This should be taken into account in the sorting order.

Similarity functions:
* inner product `InnerProductSimilarity' (sum of products of coordinates)
* cosine similarity `CosineSimilarity' (inner product / vector lengths)

Distance functions:
* euclidean distance `EuclideanDistance' (the root of the sum of squares of coordinate differences)
* cosine distance `CosineDistance' (1 - cosine similarity)

#### Function signature

```sql
Knn::InnerProductSimilarity(List<Float>?, List<Float>?)->Float?
Knn::CosineSimilarity(List<Float>?, List<Float>?)->Float?
Knn::EuclideanDistance(List<Float>?, List<Float>?)->Float?
Knn::CosineDistance(List<Float>?, List<Float>?)->Float?
```

In case of error, the functions return NULL.

### Functions for converting a vector to a binary representation

Conversion functions are needed to serialize the vector set into an internal binary representation and vice versa.
The binary representation of the vector will be stored in the YDB database in the `String` type.

#### Function signature

```sql
Knn::ToBinaryString(List<Float>)->String
Knn::FromBinaryString(String)->List<Float>?
```

In case of deserialization error, the `FromBinaryString` function return NULL.

## Examples

### Creating a table

```sql
CREATE TABLE Facts (
    id Uint64,        // Id of fact
    user String,      // User name
    fact String,      // Human readable description of user fact
    embedding String, // Binary representation of embedding vector (result of Knn::ToBinaryString)
    PRIMARY KEY (id)
)
```

### Adding vectors

```sql
UPSERT INTO Facts (id, user, fact, embedding) 
VALUES (123, "Williams", "Full name is John Williams", Knn::ToBinaryString([1.0f, 2.0f, 3.0f, 4.0f]))
```

### Exact vector search

```sql
$TargetEmbedding = [1.2f, 2.3f, 3.4f, 4.5f]

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(Knn::FromBinaryString(embedding), $TargetEmbedding)
LIMIT 10
```