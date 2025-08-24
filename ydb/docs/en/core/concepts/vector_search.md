# Vector search

## Concept of vector search

**Vector search**, also known as [nearest neighbor search](https://en.wikipedia.org/wiki/Nearest_neighbor_search) (NN), is an optimization problem where the goal is to find the nearest vector (or a set of vectors) in a given dataset relative to a specified query vector. The proximity between vectors is determined using distance or similarity metrics.

One common approach, especially for large datasets, is **approximate nearest neighbor (ANN) search**, which allows faster vector retrieval at the cost of potential accuracy trade-offs.


Vector search is actively used in the following areas:

* recommendation systems
* semantic search
* image similarity search
* anomaly detection
* classification systems

In addition, **vector search** in {{ ydb-short-name }} is widely applied in machine learning (ML) and artificial intelligence (AI) tasks. It is particularly useful in [Retrieval-Augmented Generation (RAG)](rag.md) approaches, which utilize vector search to retrieve relevant information from large volumes of data, significantly enhancing the quality of generative models. This capability is a fundamental component of {{ ydb-short-name }} as an [{#T}](ai-database.md).

Methods for solving vector search tasks can be divided into three major categories:

* [exact methods](#vector-search-exact)
* [approximate methods without index](#vector-search-approximate)
* [approximate methods with index](#vector-search-index)

The choice of a method depends on the number of vectors and the nature of the workload. Exact methods search slowly but update quickly, whereas indexes do the opposite.

## Exact vector search {#vector-search-exact}

{% include [vector_search_exact.md](../yql/reference/_includes/vector_search_exact.md) %}

Learn more about [exact vector search](../yql/reference/udf/list/knn.md#exact-vector-search-examples).

## Approximate vector search without index {#vector-search-approximate}

{% include [vector_search_approximate.md](../yql/reference/_includes/vector_search_approximate.md) %}

Learn more about [approximate vector search without index](../yql/reference/udf/list/knn.md#approximate-vector-search-examples).

## Approximate vector search with index {#vector-search-index}

When the data volume significantly increases, non-index approaches cease to work within acceptable time limits. In such cases, additional data structures are necessary such as [vector indexes](../dev/vector-indexes.md), which accelerate the search process.

Main advantage:

* ability to work with a large number of vectors

Disadvantages:

* index construction may take considerable time
* in the current version, data modification operations such as insertion, update, and deletion are not supported
