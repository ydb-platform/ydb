# KNN
## Introduction

[Nearest Neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN) is an optimization task that consists of finding the closest point in a given dataset to a given query point. Closeness can be defined in terms of distance or similarity metrics.
A generalization of the NN problem is the [k-NN problem](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm), where it's required to find the `k` nearest points to the query point. This can be useful in various applications such as image classification, recommendation systems, etc.

The k-NN problem solution is divided into two major subclasses of methods: exact and approximate.

### Exact method

The exact method is based on calculating the distance from the query point to every other point in the database. This algorithm, also known as the naive approach, has a complexity of `O(dn)`, where `n` is the number of points in the dataset, and `d` is its dimension.

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

### Approximate methods

There are a lot of approximate methods. In this document we will talk about scalar quantization.

Scalar quantization is method to compress vectors, when coordinate mapped to space with lower size.
{{ ydb-short-name }} support exact search for `Float`, `Int8`, `Uint8`, `Bit` vectors.
So it's possible to apply scalar quantization from `Float` to one of these types.

Scalar quantization decrease read/write time because we need to read/write less bytes in few times.
For an example for `Float` -> `Bit` in `32` times.

{% note info %}

It's important to remember that you probably needed to measure is it sufficient accuracy/recall or not.

{% endnote %}

## Data types

In mathematics, a vector of real numbers is used to store points.
In {{ ydb-short-name }}, calculations are performed on the `String` data type, which is a binary serialized representation of `List<Float>`.

## Functions

Vector functions are implemented as user-defined functions (UDF) in the `Knn` module.

### Functions for converting between vector and binary representations

Conversion functions are needed to serialize the vector set into an internal binary representation and vice versa.

All conversation functions returns [Tagged](../../types/special.md) types.

The binary representation of a vector can be persisted in a {{ ydb-short-name }} table column, {{ ydb-short-name }} doesn't support storing `Tagged` types, so user should store them as `String` type using [Untag](../../builtins/basic#as-tagged) function.

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

* `ToBinaryStringBit` -- coordinates that greater than `0` are mapped to `1`, other coordinates are mapped to `0`.
* `ToBinaryStringBit`, `ToBinaryStringUint8`, `ToBinaryStringInt8` -- commonly used for quantization.

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

All distance and similarity functions support overloads when first or second arguments are `Tagged<String, "FloatVector">`, `Tagged<String, "Uint8Vector">`, `Tagged<String, "Int8Vector">`, `Tagged<String, "BitVector">`.

If both arguments are `Tagged`, value of tag should be same, overwise there will be an error for this request.

{% endnote %}

## Examples of exact search

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

## Examples of approximate search

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

### Quantization

Quantization can be done by ML model, or this can be done in YQL.

Here examples of quantization in YQL.

#### Float -> Int8

```sql
$MapInt8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $dist = $max - $min;
	RETURN Cast(Math::Round(IF($x < $min, -127, IF($x > $max, 127, ($x / $dist) * 255))) As Int8)
};
```

#### Float -> Uint8

```sql
$MapUint8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $dist = $max - $min;
	RETURN Cast(Math::Round(IF($x < $min, 0, IF($x > $max, 255, (($x - $min) / $dist) * 255))) As Uint8)
};
```

#### Usage

```sql
$FloatList = [-1.2f, 2.3f, 3.4f, -4.7f];
select ListMap($FloatList, $MapInt8), ListMap($Target, $MapUint8);
```

### Approximate search of K nearest vectors

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
