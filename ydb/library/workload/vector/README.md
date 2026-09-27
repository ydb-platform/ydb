# Vector workload index selection

`build-index` and both `import` modes accept `--index-type distributed_hnsw`
(or `DistributedHnsw`). The default remains `KmeansTree`; `vector_kmeans_tree`
is also accepted. `None` skips index creation after import.

For an existing workload table with float vectors:

```sh
ydb -e grpc://localhost:2135 -d /Root/testdb workload vector build-index \
    --table vectors --index hnsw --index-type distributed_hnsw \
    --vector-type float --vector-dimension 200 \
    --kmeans-tree-levels 1 --kmeans-tree-clusters 10 \
    --hnsw-min-rows 1 --hnsw-search-candidates 50

ydb -e grpc://localhost:2135 -d /Root/testdb workload vector run select \
    --table vectors --index hnsw --threads 50 \
    --targets 100 --limit 10 --kmeans-tree-clusters 10 --recall
```

Use `build-index --dry-run` to inspect the generated DDL. To build during data
import, pass the same index and HNSW options to `import files` or `import generator`.
Run `workload vector import --help` for available data initializers.

`run select` reads vector settings from the named index and uses the same query,
throughput, latency, and optional recall measurements for either index type.
`--query-table` selects predefined query vectors. `--non-indexed` measures the
brute-force baseline; `--stale-ro` uses stale reads, including configured replicas.

HNSW creation options (stored on the index):

| Option | Default |
| --- | --- |
| `--hnsw-min-rows` | 10000 |
| `--hnsw-connectivity` | 16 |
| `--hnsw-construction-candidates` | 200 |
| `--hnsw-search-candidates` | 15 |
| `--hnsw-rebuild-threshold-percent` | 10 |

Partitions below `--hnsw-min-rows` use brute-force search. HNSW needs a memory
controller cache allocation; cold or rebuilding caches can affect performance.
The HNSW creation options apply only to `distributed_hnsw`. Recreate the index
with different options to compare HNSW configurations.
