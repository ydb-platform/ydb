The foundation of the exact method is the calculation of the distance from the query vector to all the vectors in the dataset. This algorithm, also known as the naive approach or brute force method, has a runtime of `O(dn)`, where `n` is the number of vectors in the dataset, and `d` is their dimensionality.

[Exact vector search](../udf/list/knn.md#exact-vector-search-examples) is best utilized if the complete enumeration of the vectors occurs within acceptable time limits. This includes cases where they can be pre-filtered based on some condition, such as a user identifier. In such instances, the exact method may perform faster than the current implementation of [vector indexes](../../../dev/vector-indexes.md).

Main advantages:

* No need for additional data structures, such as specialized [vector indexes](../../../concepts/glossary.md#vector-index).
* Full support for [transactions](../../../concepts/glossary.md#transactions), including in strict consistency mode.
* Instant execution of data modification operations: insertion, update, deletion.
