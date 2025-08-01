# KNN

## Introduction {#introduction}

One specific case of [vector search](../../../../concepts/vector_search.md) is the [k-NN](https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm) problem, where it is required to find the `k` nearest points to the query point. This can be useful in various applications such as image classification, recommendation systems, etc.

The k-NN problem solution is divided into two major subclasses of methods: exact and approximate.

### Exact method {#exact-method}

{% include [vector_search_exact.md](../../_includes/vector_search_exact.md) %}

### Approximate methods {#approximate-methods}

{% include [vector_search_approximate.md](../../_includes/vector_search_approximate.md) %}

{% note info %}

It is recommended to measure if such quantization provides sufficient accuracy/recall.

{% endnote %}

## Data types {#data-types}

In mathematics, a vector of real or integer numbers is used to store points.
In this module, vectors are stored in the `String` data type, which is a binary serialized representation of a vector.

## Functions {#functions}

Vector functions are implemented as user-defined functions (UDF) in the `Knn` module.

### Functions for converting between vector and binary representations {#functions-convert}

Conversion functions are needed to serialize vectors into an internal binary representation and vice versa.

All serialization functions wrap returned `String` data into [Tagged](../../types/special.md) types.

{% if backend_name == "YDB" %}

The binary representation of the vector can be stored in the {{ ydb-short-name }} table column.

{% note info %}

Currently {{ ydb-short-name }} does not support storing `Tagged`, so before storing binary representation vectors you must call [Untag](../../builtins/basic#as-tagged).

{% endnote %}

{% note info %}

Currently {{ ydb-short-name }} does not support building an index for vectors with bit quantization `BitVector`.

{% endnote %}

{% endif %}

#### Function signatures {#functions-convert-signature}

```yql
Knn::ToBinaryStringFloat(List<Float>{Flags:AutoMap})->Tagged<String, "FloatVector">
Knn::ToBinaryStringUint8(List<Uint8>{Flags:AutoMap})->Tagged<String, "Uint8Vector">
Knn::ToBinaryStringInt8(List<Int8>{Flags:AutoMap})->Tagged<String, "Int8Vector">
Knn::ToBinaryStringBit(List<Double>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Float>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Uint8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::ToBinaryStringBit(List<Int8>{Flags:AutoMap})->Tagged<String, "BitVector">
Knn::FloatFromBinaryString(String{Flags:AutoMap})->List<Float>?
```

#### Convert format {#functions-convert-format}

Conversion functions for vector data convert an array of elements into a byte string with the following format:

- **Main part** — a contiguous array of elements ([knn-serializer.h](https://github.com/ydb-platform/ydb/blob/0b506f56e399e0b4e6a6a4267799da68a3164bf7/ydb/library/yql/udfs/common/knn/knn-serializer.h#L19))
- **Type** — 1 byte at the end of the string that specifies the data type ([knn-defines.h](https://github.com/ydb-platform/ydb/blob/24026648dd7463d58e1470aa8981b17677116e7c/ydb/library/yql/udfs/common/knn/knn-defines.h#L5)):  
  `1` — `Float` (4 bytes per element)
  `2` — `Uint8` (1 byte per element)
  `3` — `Int8` (1 byte per element)
  `10` — `Bit` (1 byte per element)

For example, a vector of 5 elements of type `Float` will be serialized into a 21-byte string:  
4 bytes × 5 elements (main part) + 1 byte (type) = 21 bytes.

#### Implementation details {#functions-convert-details}

The `ToBinaryStringBit` function maps coordinates that are greater than `0` to `1`. All other coordinates are mapped to `0`.

### Distance and similarity functions {#functions-distance}

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

#### Function signatures {#functions-distance-signatures}

```yql
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

```text
Error: Failed to find UDF function: Knn.CosineDistance, reason: Error: Module: Knn, function: CosineDistance, error: Arguments should have same tags, but 'FloatVector' is not equal to 'Uint8Vector'
```

{% endnote %}

## Exact search examples {#exact-vector-search-examples}

{% if backend_name == "YDB" %}

### Creating a table {#exact-vector-search-examples-create}

```yql
CREATE TABLE Facts (
    id Uint64,        -- Id of fact
    user Utf8,        -- User name
    fact Utf8,        -- Human-readable description of a user fact
    embedding String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    PRIMARY KEY (id)
);
```

### Adding vectors {#exact-vector-search-examples-upsert}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"));
```

{% else %}

### Data declaration {#exact-vector-search-examples-create-list}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
$facts = AsList(
    AsStruct(
        123 AS id,  -- Id of fact
        "Williams" AS user,  -- User name
        "Full name is John Williams" AS fact,  -- Human-readable description of a user fact
        Knn::ToBinaryStringFloat($vector) AS embedding,  -- Binary representation of embedding vector
    ),
);
```

{% endif %}

### Exact search of K nearest vectors {#exact-vector-search-k-nearest}

{% if backend_name == "YDB" %}

```yql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

{% else %}

```yql
$K = 10;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM AS_TABLE($facts)
WHERE user="Williams"
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

{% endif %}

### Exact search of vectors in radius R {#exact-vector-search-radius}

{% if backend_name == "YDB" %}

```yql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM Facts
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

{% else %}

```yql
$R = 0.1f;
$TargetEmbedding = Knn::ToBinaryStringFloat([1.2f, 2.3f, 3.4f, 4.5f]);

SELECT * FROM AS_TABLE($facts)
WHERE Knn::CosineDistance(embedding, $TargetEmbedding) < $R;
```

{% endif %}

## Approximate search examples {#approximate-vector-search-examples}

This example differs from the [exact search example](#exact-vector-search-examples) by using bit quantization.

This allows to first do a approximate preliminary search by the `embedding_bit` column, and then refine the results by the original vector column `embegging`.

{% if backend_name == "YDB" %}

### Creating a table {#approximate-vector-search-examples-create}

```yql
CREATE TABLE Facts (
    id Uint64,            -- Id of fact
    user Utf8,            -- User name
    fact Utf8,            -- Human-readable description of a user fact
    embedding String,     -- Binary representation of embedding vector (result of Knn::ToBinaryStringFloat)
    embedding_bit String, -- Binary representation of embedding vector (result of Knn::ToBinaryStringBit)
    PRIMARY KEY (id)
);
```

### Adding vectors {#approximate-vector-search-examples-upsert}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
UPSERT INTO Facts (id, user, fact, embedding, embedding_bit)
VALUES (123, "Williams", "Full name is John Williams", Untag(Knn::ToBinaryStringFloat($vector), "FloatVector"), Untag(Knn::ToBinaryStringBit($vector), "BitVector"));
```

{% else %}

### Data declaration {#approximate-vector-search-examples-create-list}

```yql
$vector = [1.f, 2.f, 3.f, 4.f];
$facts = AsList(
    AsStruct(
        123 AS id,  -- Id of fact
        "Williams" AS user,  -- User name
        "Full name is John Williams" AS fact,  -- Human-readable description of a user fact
        Knn::ToBinaryStringFloat($vector) AS embedding,  -- Binary representation of embedding vector
        Knn::ToBinaryStringBit($vector) AS embedding_bit,  -- Binary representation of embedding vector
    ),
);
```

{% endif %}

### Scalar quantization {#approximate-vector-search-scalar-quantization}

An ML model can do quantization, or it can be done manually with YQL.

Below there is a quantization example in YQL.

#### Float -> Int8 {#approximate-vector-search-scalar-quantization-map}

```yql
$MapInt8 = ($x) -> {
    $min = -5.0f;
    $max =  5.0f;
    $range = $max - $min;
  RETURN CAST(Math::Round(IF($x < $min, -127, IF($x > $max, 127, ($x / $range) * 255))) As Int8)
};

$FloatList = [-1.2f, 2.3f, 3.4f, -4.7f];
SELECT ListMap($FloatList, $MapInt8);
```

### Approximate search of K nearest vectors: bit quantization {#approximate-vector-search-scalar-quantization-example}

Approximate search algorithm:

* an approximate search is performed using bit quantization;
* an approximate list of vectors is obtained;
* we search this list without using quantization.

{% if backend_name == "YDB" %}

```yql
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

{% else %}

```yql
$K = 10;
$Target = [1.2f, 2.3f, 3.4f, 4.5f];
$TargetEmbeddingBit = Knn::ToBinaryStringBit($Target);
$TargetEmbeddingFloat = Knn::ToBinaryStringFloat($Target);

$Ids = SELECT id FROM AS_TABLE($facts)
ORDER BY Knn::CosineDistance(embedding_bit, $TargetEmbeddingBit)
LIMIT $K * 10;

SELECT * FROM AS_TABLE($facts)
WHERE id IN $Ids
ORDER BY Knn::CosineDistance(embedding, $TargetEmbeddingFloat)
LIMIT $K;
```

{% endif %}
