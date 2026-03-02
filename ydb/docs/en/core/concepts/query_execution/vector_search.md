# Vector search

## Vector search concept

**Vector search**, also known as [nearest neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN), is an optimization problem of finding the nearest vector (or set of vectors) in a given dataset relative to a query vector. Proximity between vectors is determined using distance or similarity metrics.

A common approach, especially with large datasets, is approximate nearest neighbor (ANN) search, which yields faster results at the possible cost of some accuracy.

Vector search is widely used in:

* recommendation systems;
* semantic search;
* image similarity search;
* anomaly detection;
* classification systems.

In addition, **vector search** in {{ ydb-short-name }} is widely applied in machine learning (ML) and artificial intelligence (AI) tasks. It is particularly useful in Retrieval-Augmented Generation (RAG) approaches, which utilize vector search to retrieve relevant information from large volumes of data, significantly enhancing the quality of generative models.

Vector search methods can be divided into three main categories:

* [exact methods](#vector-search-exact);
* [approximate methods without index](#vector-search-approximate);
* [approximate methods with index](#vector-search-index).

The choice of method depends on the number of vectors and the workload. Exact methods are slower to search and faster to update; indexes are the opposite.

## Exact vector search {#vector-search-exact}

{% include [vector_search_exact.md](../../yql/reference/_includes/vector_search_exact.md) %}

For more information, see [exact vector search examples](../../yql/reference/udf/list/knn.md#exact-vector-search-examples).

## Approximate vector search without index {#vector-search-approximate}

{% include [vector_search_approximate.md](../../yql/reference/_includes/vector_search_approximate.md) %}

Learn more about [approximate vector search without index](../../yql/reference/udf/list/knn.md#approximate-vector-search-examples).

## Approximate vector search with index {#vector-search-index}

When the data volume significantly increases, non-index approaches cease to work within acceptable time limits. In such cases, additional data structures are necessary such as [vector indexes](../../dev/vector-indexes.md), which accelerate the search process.

Main advantage:

* ability to work with a large number of vectors.

Disadvantages:

* index build can take considerable time;
* the current version does not support data modification operations: insert, update, delete.
