# KNN
## Introduction

[Nearest Neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) is an optimization task that consists of finding the closest point in a given dataset to a given query point. Closeness can be defined in terms of distance or similarity metrics.
A generalization of the NN problem is the [k-NN problem](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), where it's required to find the `k` nearest points to the query point. This can be useful in various applications such as image classification, recommendation systems, etc.

The k-NN problem solution is divided into two major subclasses of methods: exact and approximate. In this document, we will talk about the exact brute force method.

This method is based on calculating the distance from the query point to every other point in the database. This algorithm, also known as the naive approach, has a complexity of `O(dn)`, where `n` is the number of points in the dataset, and `d` is its dimension.

The advantage of the method is that there is no need for additional data structures, such as specialized vector indexes.
The disadvantage is the need for a complete data search. But this disadvantage is insignificant in cases where data has been pre-filtered, for example, by user ID.

Example:

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10;
```

## Data types

In mathematics, a vector of real numbers is used to store points.
In {{ ydb-short-name }}, calculations are performed on the `String` data type, which is a binary serialized representation of `List<Float>`.

## Functions

Vector functions are implemented as user-defined functions (UDF) in the `Knn` module.

### Distance and similarity functions

The distance and similarity functions take two lists of real numbers as input and return the distance/similarity between them.

{% note info %}

Distance functions return small values for close vectors, while similarity functions return large values for close vectors. This should be taken into account when defining the sorting order.

{% endnote %}

Similarity functions:
* inner product `InnerProductSimilarity`, it's `dot product` for us, also known as `scalar product` (sum of products of coordinates)
* cosine similarity `CosineSimilarity` (inner product / product of vector lengths)

Distance functions:
* cosine distance `CosineDistance` (1 - cosine similarity)
* mahattan distance `ManhattanDistance`, also known as `L1 distance` (sum of modules of coordinate difference)
* euclidean distance `EuclideanDistance`, also known as `L2 distance` (square root of sum of squares of coordinate difference)

#### Function signatures

```sql
Knn::InnerProductSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::ManhattanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::EuclideanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
```

In case of different length or format, these functions return `NULL`.

{% note info %}

All distance and similarity functions support overloads when first or second argument
can be `Tagged<String, "FloatVector">`, `Tagged<String, "Uint8Vector">`, `Tagged<String, "Int8Vector">`, `Tagged<String, "BitVector">`.

If both arguments are `Tagged`, value of tag should be same, overwise will be error for this request.

{% endnote %}


### Functions for converting between vector and binary representations

Conversion functions are needed to serialize the vector set into an internal binary representation and vice versa.
The binary representation of a vector can be persisted in a {{ ydb-short-name }}  table column of the `String` type.

#### Function signatures

```sql
Knn::ToBinaryStringFloat(List<Float>{Flags:AutoMap})->Tagged<String, "FloatVector">
Knn::ToBinaryStringUint8(List<Uint8>{Flags:AutoMap})->Tagged<String, "Uint8Vector">
Knn::ToBinaryStringInt8(List<Int8>{Flags:AutoMap})->Tagged<String, "Int8Vector">
Knn::ToBinaryStringBit(List<Double>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Float>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Uint8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Int8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::FloatFromBinaryString(String{Flags:AutoMap})->List<Float>?
```

* `ToBinaryStringBit` -- translate to `1` all coordinates that greater than `0`, other coordinates translated to `0`.
* `ToBinaryStringBit`, `ToBinaryStringUint8`, `ToBinaryStringInt8` -- commonly used for quantization.

## Examples

### Creating a table

```sql
CREATE TABLE Facts (
    id Uint64,        -- Id of fact
    user Utf8,        -- User name
    fact Utf8,        -- Human-readable description of a user fact
    embedding String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    embedding_bit String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringBit)
    PRIMARY KEY (id)
);
```

### Adding vectors

```sql
$vector = CAST([1, 2, 3, 4] AS List<Float>);
UPSERT INTO Facts (id, user, fact, embedding, embedding_bit) 
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"), Untag(Knn::ToBinaryStringBit($vector), "BitVector"));
```

{% note info %}

{{ ydb-short-name }} doesn't support storing `Tagged` types, so user should store them as `String`, to achive this user can use `Untag` function.

{% endnote %}

### Exact vector search

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10;
```

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < 0.1;
```


### Approximate neareast neighbor vector search: quantization

```sql
CREATE TABLE Facts (
    id Uint64 NOT NULL,
    embedding String,
    embedding_bit String,
    PRIMARY KEY (id)
);

INSERT INTO my_table VALUES(
    (1, Untag(Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]), "FloatVector"), Untag(Knn::ToBinaryStringBit([1.2f, 2.3f, 3.4f, 4.5f])), "BitVector")
);
```

```sql
$TargetEmbeddingBit = Knn::ToBinaryStringBit([1.2f, 2.3f, 3.4f, 4.5f]);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

$Ids = SELECT id FROM Facts
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT 100;

SELECT * FROM Facts
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT 10;
```
