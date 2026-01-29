# USearch for C++

## Installation

To use in a C++ project, copy the `include/usearch/*` headers into your project.
Alternatively, fetch it with CMake:

```cmake
FetchContent_Declare(usearch GIT_REPOSITORY https://github.com/unum-cloud/usearch.git)
FetchContent_MakeAvailable(usearch)
```

## Quickstart

Once included, the high-level C++11 interface is as simple as it gets: `reserve()`, `add()`, `search()`, `size()`, `capacity()`, `save()`, `load()`, `view()`.
This covers 90% of use cases.

```cpp
#include <usearch/index.hpp>
#include <usearch/index_dense.hpp>

using namespace unum::usearch;

int main(int argc, char **argv) {
    metric_punned_t metric(3, metric_kind_t::l2sq_k, scalar_kind_t::f32_k);
    
    // If you plan to store more than 4 Billion entries - use `index_dense_big_t`.
    // Or directly instantiate the template variant you need - `index_dense_gt<vector_key_t, internal_id_t>`.
    index_dense_t index = index_dense_t::make(metric);
    float vec[3] = {0.1, 0.3, 0.2};
    
    index.reserve(10); // Pre-allocate memory for 10 vectors
    index.add(42, &vec[0]); // Pass a key and a vector
    auto results = index.search(&vec[0], 5); // Pass a query and limit number of results
    
    for (std::size_t i = 0; i != results.size(); ++i)
        // You can access the following properties of every match:
        // results[i].element.key, results[i].element.vector, results[i].distance;
        std::printf("Found matching key: %zu", results[i].member.key);
    return 0;
}
```

Here we:

- define a metric of kind [`metric_kind_t::l2sq_k`](https://unum-cloud.github.io/usearch/cpp/reference.html#_CPPv413metric_kind_t),
- to be applied to [`scalar_kind_t::f32_k`](https://unum-cloud.github.io/usearch/cpp/reference.html#_CPPv413scalar_kind_t) floating-point vectors,
- instantiate an [`index_dense_t`](https://unum-cloud.github.io/usearch/cpp/reference.html#_CPPv4I00EN4unum7usearch14index_dense_gtE) index.

The `add` is thread-safe for concurrent index construction.
It also has an overload for different vector types, casting them under the hood.
The same applies to the `search`, `get`, `cluster`, and `distance_between` functions.

```cpp
double vec_double[3] = {0.1, 0.3, 0.2};
_Float16 vec_half[3] = {0.1, 0.3, 0.2};
index.add(43, {&vec_double[0], 3});
index.add(44, {&vec_half[0], 3});
```

## Serialization

```cpp
index.save("index.usearch");
index.load("index.usearch"); // Copying from disk
index.view("index.usearch"); // Memory-mapping from disk
```

## Multi-Threading

Most AI, HPC, or Big Data packages use some form of a thread pool.
Instead of spawning additional threads within USearch, we focus on the thread safety of `add()` function, simplifying resource management.

```cpp
#pragma omp parallel for
    for (std::size_t i = 0; i < n; ++i)
        native.add(key, span_t{vector, dims});
```

During initialization, we allocate enough temporary memory for all the cores on the machine.
On the call, the user can supply the identifier of the current thread, making this library easy to integrate with OpenMP and similar tools.
Here is how parallel indexing may look like, when dealing with the low-level engine:

```cpp
std::size_t executor_threads = std::thread::hardware_concurrency() * 4;
executor_default_t executor(executor_threads);

index.reserve(index_limits_t {vectors.size(), executor.size()});
executor.fixed(vectors.size(), [&](std::size_t thread, std::size_t task) {
    index.add(task, vectors[task + 3].data(), index_update_config_t { .thread = thread });
});
```

Aside from the `executor_default_t`, you can take advantage of one of the provided "executors" to parallelize the search:

- `executor_openmp_t`, that would use OpenMP under the hood.
- `executor_stl_t`, that will spawn `std::thread` instances.
- `dummy_executor_t`, that will run everything sequentially.

## Error Handling

Unlike most open-source C++ libraries USearch doesn't use exceptions, as they often lead to corrupted states in concurrent data-structures.
They are also not suited for systems that always operate on the edge of available memory, and would end up raising `std::bad_alloc` exceptions all the time.
For that reason, most operations return a "result" object, that can be checked for success or failure.

```cpp
bool success;
success = (bool)index.try_reserve(10); // Recommended over `reserve()`
success = (bool)index.add(42, &vec[0]); // Explicitly convert `add_result_t`
success = (bool)index.search(&vec[0], 5); // Explicitly convert `search_result_t`
```

## Clustering

Aside from basic Create-Read-Update-Delete (CRUD) operations and search, USearch also supports clustering.
Once the index is constructed, you can either:

- Identify a cluster to which any external vector belongs, once mapped onto the index.
- Split the entire index into a set of clusters, each with its own centroid.

For the first, the interface accepts a vector and a "clustering level", which is essentially the index of the HNSW graph layer, in which to search.
If you pass zero, the traversal will happen in every level except the bottom one.
Otherwise, the search will be limited to the specified level.

```cpp
some_scalar_t vector[3] = {0.1, 0.3, 0.2};
cluster_result_t result = index.cluster(&vector, index.max_level() / 2);
match_t cluster = result.cluster;
member_cref_t member = cluster.member;
distance_t distance = cluster.distance;
```

If you wish to split the whole structure into clusters, you must provide an iterator over a range of vectors, that will be processed in parallel using the previously described function.
Unlike the previous function, you don't have to manually specify the level, as the algorithm will pick the best one for you, depending on the number of clusters you want to highlight.
Aside from that auto-tuning, this function will regroup some of the clusters, if they are too small, and return the final number of clusters.

```cpp
std::size_t queries_count = queries_end - queries_begin;
index_dense_clustering_config_t config;
config.min_clusters = 1000;
config.max_clusters = 2000;
config.mode = index_dense_clustering_config_t::merge_smallest_k;

// Outputs:
vector_key_t cluster_centroids_keys[queries_count];
distance_t distances_to_cluster_centroids[queries_count];
executor_default_t thread_pool;
dummy_progress_t progress_bar;

clustering_result_t result = cluster(
        queries_begin, queries_end,
        config,
        &cluster_centroids_keys, &distances_to_cluster_centroids,
        thread_pool, progress_bar);
```

This approach requires basic understanding of templates meta-programming to implement the `queries_begin` and `queries_end` smart-iterators.
On the bright side, it allows iteratively deepening into a specific cluster.

As in many other bulk-processing APIs, the `executor` and `progress` are optional.

## User-Defined Metrics

In its high-level interface, USearch supports a variety of metrics, including the most popular ones:

- `metric_cos_gt<scalar_t>` for "Cosine" or "Angular" distance.
- `metric_ip_gt<scalar_t>` for "Inner Product" or "Dot Product" distance with normalized vectors.
- `metric_l2sq_gt<scalar_t>` for the squared "L2" or "Euclidean" distance.
- `metric_jaccard_gt<scalar_t>` for "Jaccard" distance between two ordered sets of unique elements.
- `metric_hamming_gt<scalar_t>` for "Hamming" distance, as the number of shared bits in hashes.
- `metric_tanimoto_gt<scalar_t>` for "Tanimoto" coefficient for bit-strings.
- `metric_sorensen_gt<scalar_t>` for "Dice-Sorensen" coefficient for bit-strings.
- `metric_pearson_gt<scalar_t>` for "Pearson" correlation between probability distributions.
- `metric_haversine_gt<scalar_t>` for "Haversine" or "Great Circle" distance between coordinates used in GIS applications.
- `metric_divergence_gt<scalar_t>` for the "Jensen Shannon" similarity between probability distributions.

In reality, for most common types, one of the [SimSIMD](https://github.com/ashvardanian/SimSIMD) backends will be triggered, providing hardware-acceleration for most common CPUs.

If you need a different metric, you can implement it yourself and wrap it into a `metric_punned_t`, which is our alternative to the `std::function`.
Unlike the `std::function`, it is a trivial type, which is important for performance.

## Advanced Interface

If you are proficient in C++ and ready to get your hands dirty, you can use the low-level interface.

```cpp
template <typename distance_at = default_distance_t,              // `float`
          typename key_at = default_key_t,                        // `int64_t`, `uuid_t`
          typename compressed_slot_at = default_slot_t,           // `uint32_t`, `uint40_t`
          typename dynamic_allocator_at = std::allocator<byte_t>, //
          typename tape_allocator_at = dynamic_allocator_at>      //
class index_gt;
```
