# KNN
## Introduction

[Nearest Neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) is an optimization task that consists of finding the closest point in a given dataset to a given query point. Closeness can be defined in terms of distance or similarity metrics.
A generalization of the NN problem is the [k-NN problem](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), where it's required to find the `k` nearest points to the query point. This can be useful in various applications such as image classification, recommendation systems, etc.

The k-NN problem solution is divided into two major subclasses of methods: exact and approximate.

### Exact method

The exact method is based on calculating the distance from the query point to every other point in the database. This algorithm, also known as the naive approach, has a complexity of `O(dn)`, where `n` is the number of points in the dataset, and `d` is its dimension.

The advantage of the method is that there is no need for additional data structures, such as specialized vector indexes.
The disadvantage is the need for a full data scan. But this disadvantage is insignificant in cases where data has been pre-filtered, for example, by user ID.

Example:

```sql
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT id, fact, embedding FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT 10;
```

### Approximate methods

Approximate methods do not perform a complete search of the source data. Due to this, they work significantly faster, although they may result in some loss of quality.

This document provides an [example of approximate search](#approximate-search-examples) using scalar quantization. This example does not require the creation of a secondary vector index.

**Scalar quantization** is a method to compress vectors by mapping coordinates to a smaller space.
{{ ydb-short-name }} support exact search for `Float`, `Int8`, `Uint8`, `Bit` vectors.
So, it's possible to apply scalar quantization from `Float` to one of these other types.

Scalar quantization decreases read/write times by reducing vector size in bytes. For example, after quantization from `Float` to `Bit,` each vector becomes 32 times smaller.

{% note info %}

It is recommended to measure if such quantization provides sufficient accuracy/recall.

{% endnote %}

## Data types

In mathematics, a vector of real or integer numbers is used to store points.
In {{ ydb-short-name }}, vectors are stored in the `String` data type, which is a binary serialized representation of a vector.

## Functions

Vector functions are implemented as user-defined functions (UDF) in the `Knn` module.

### Functions for converting between vector and binary representations

Conversion functions are needed to serialize vectors into an internal binary representation and vice versa.

All serialization functions wrap returned `String` data into [Tagged](../../types/special.md) types.

The binary representation of the vector can be stored in the {{ ydb-short-name }} table column. Currently {{ ydb-short-name }} does not support storing `Tagged`, so before storing binary representation vectors you must call [Untag](../../builtins/basic#as-tagged).

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

#### Implementation details

The `ToBinaryStringBit` function maps coordinates that are greater than `0` to `1`. All other coordinates are mapped to `0`.

### Distance and similarity functions

The distance and similarity functions take two lists of real numbers as input and return the distance/similarity between them.

{% note info %}

Distance functions return small values for close vectors, while similarity functions return large values for close vectors. This should be taken into account when defining the sorting order.

{% endnote %}

Similarity functions:
* inner product `InnerProductSimilarity`, it's the dot product, also known as the scalar product (sum of products of coordinates)
* cosine similarity `CosineSimilarity` (dot product divided by product of vector lengths)

Distance functions:
* cosine distance `CosineDistance` (1 - cosine similarity)
* manhattan distance `ManhattanDistance`, also known as `L1 distance` (sum of modules of coordinate differences)
* euclidean distance `EuclideanDistance`, also known as `L2 distance` (square root of the sum of squares of coordinate differences)

#### Function signatures

```sql
Knn::InnerProductSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineSimilarity(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::CosineDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::ManhattanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
Knn::EuclideanDistance(String{Flags:AutoMap}, String{Flags:AutoMap})->Float?
```

In case of mismatched lengths or formats, these functions return `NULL`.

{% note info %}

All distance and similarity functions support overloads when first or second arguments are `Tagged<String, "FloatVector">`, `Tagged<String, "Uint8Vector">`, `Tagged<String, "Int8Vector">`, `Tagged<String, "BitVector">`.

If both arguments are `Tagged`, tag values should match, or the query will raise an error.

Example:

```
Error: Failed to find UDF function: Knn.CosineDistance, reason: Error: Module: Knn, function: CosineDistance, error: Arguments should have same tags, but 'FloatVector' is not equal to 'Uint8Vector'
```

{% endnote %}

## Еxact search examples

### Creating a table

```sql
CREATE TABLE Facts (
    id Uint64,        -- Id of fact
    user Utf8,        -- User name
    fact Utf8,        -- Human-readable description of a user fact
    embedding String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    PRIMARY KEY (id)
);
```

### Adding vectors

```sql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));
```

### Exact search of K nearest vectors

```sql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

### Exact search of vectors in radius R

```sql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

## Approximate search examples

This example differs from the [exact search example](#еxact-search-examples) by using bit quantization.

This allows to first do a approximate preliminary search by the `embedding_bit` column, and then refine the results by the original vector column `embegging`.

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
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding, embedding_bit)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"), Untag(Knn::ToBinaryStringBit($vector), "BitVector"));
```

### Scalar quantization

An ML model can do quantization, or it can be done manually with YQL.

Below there is a quantization example in YQL.

#### Float -> Int8

```sql
$MapInt8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $range = $max - $min;
	RETURN CAST(Math::Round(IF($x < $min, -127, IF($x > $max, 127, ($x / $range) * 255))) As Int8)
};

$FloatList = [-1.2f, 2.3f, 3.4f, -4.7f];
SELECT ListMap($FloatList, $MapInt8);
```

### Approximate search of K nearest vectors: bit quantization

Approximate search algorithm:
* an approximate search is performed using bit quantization;
* an approximate list of vectors is obtained;
* we search this list without using quantization.

```sql
$K = 10;
$Target = [1.2f, 2.3f, 3.4f, 4.5f];
$TargetEmbeddingBit = Knn::ToBinaryStringBit($Target);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);

$Ids = SELECT id FROM Facts
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT $K * 10;

SELECT * FROM Facts
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT $K;
```
