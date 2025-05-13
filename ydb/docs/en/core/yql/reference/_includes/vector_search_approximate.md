Approximate methods do not perform a complete enumeration of the initial data. This allows significantly speeding up the search process, although it might lead to some reduction in the quality of the results.

[Scalar Quantization](../udf/list/knn.md#approximate-vector-search-scalar-quantization) is a method of reducing vector dimensionality, where a set of coordinates is mapped into a space of smaller dimensions.

{{ ydb-short-name }} supports vector searching for vector types `Float`, `Int8`, `Uint8`, and `Bit`. Consequently, it is possible to apply scalar quantization to transform data from `Float` to any of these types.

Scalar quantization reduces the time required for reading and writing data by decreasing the number of bytes. For example, when quantizing from `Float` to `Bit`, each vector is reduced by 32 times.

[Approximate vector search without an index](../udf/list/knn.md#approximate-vector-search-examples) uses a very simple additional data structure - a set of vectors with other quantization. This allows the use of a simple search algorithm: first, a rough preliminary search is performed on the compressed vectors, followed by refining the results on the original vectors.

Main advantages:

* Full support for [transactions](../../../concepts/glossary.md#transactions), including in strict consistency mode.
* Instant application of data modification operations: insertion, update, deletion.
