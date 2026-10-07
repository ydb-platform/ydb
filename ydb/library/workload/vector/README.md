# Vector workload index selection

`build-index` and both `import` modes accept `--index-type hnsw`
(or `Hnsw`). The default remains `KmeansTree`; `vector_kmeans_tree`
is also accepted. `None` skips index creation after import.

For an existing workload table with float vectors:

```sh
ydb -e grpc://localhost:2135 -d /Root/testdb workload vector build-index \
    --table vectors --index hnsw --index-type hnsw \
    --vector-type float --vector-dimension 200 \
    --kmeans-tree-levels 1 --kmeans-tree-clusters 10 \
    --min-rows 1 --M 16 --ef-construction 200 --delta-rows 10000

ydb -e grpc://localhost:2135 -d /Root/testdb workload vector run select \
    --table vectors --index hnsw --threads 50 \
    --targets 100 --limit 10 --ef-search 50 --recall
```

Use `build-index --dry-run` to inspect the generated DDL. To build during data
import, pass the same index and HNSW options to `import files` or `import generator`.
Run `workload vector import --help` for available data initializers.

`run select` reads vector settings from the named index and uses the same query,
throughput, latency, and optional recall measurements for either index type.
`--query-table` selects predefined query vectors. `--non-indexed` measures the
brute-force baseline; `--stale-ro` uses stale reads, including configured replicas. HNSW
selects use read-only snapshots by default: SerializableRW reads may take locks
that force scan fallback. Each concurrent worker atomically selects its next
query vector.

HNSW creation options (stored on the index):

| Option | Default |
| --- | --- |
| `--min-rows` | 10000 |
| `--M` | 16 |
| `--ef-construction` | 200 |
| `--delta-rows` | 10000 |

Partitions below `--min-rows` use brute-force search. HNSW needs a memory
controller cache allocation; cold or rebuilding caches can affect performance.
The HNSW creation options apply only to `hnsw`. Recreate the index
with different options to compare HNSW configurations.

Unprefixed `hnsw` searches use the named index `VIEW`. The server
searches every posting partition's HNSW graph in parallel and merges the top-K
results, using the stored HNSW settings. K-means cluster pruning does not apply
to these searches. Prefix indexes retain their prefix filter and cluster traversal;
overlapping postings are deduplicated by the server.

Use `--stale-ro` to distribute reads across configured read replicas, or omit it
for snapshot-consistent reads. Warm the graphs before measuring steady-state
throughput. The existing index can be reused after upgrading the server.

Use `--non-indexed` to search the base table for a brute-force comparison.

The SQL names are `min_rows`, `M`, `ef_construction`, and `delta_rows`.
`delta_rows` is an absolute limit on distinct rows with committed vector changes
since the graph snapshot, per partition. Repeated writes to the same row count
once. A rebuild starts when the count exceeds the limit; zero triggers a rebuild
after any changed row. The default is 10000, independent of the graph size.

The old percentage field is reserved in the wire format. An existing index with
only that old field uses the new default until `delta_rows` is explicitly set.
Search breadth is configured per query, with a default of 15:

```sql
PRAGMA ydb.HNSWEfSearch = "15";
SELECT id FROM vectors VIEW hnsw
ORDER BY Knn::InnerProductSimilarity(embedding, $query) DESC LIMIT 10;
```

`run select --ef-search 50` emits that pragma with value 50. Valid values are
1..1000. Search breadth is not stored on the index, and changing it reuses the
same cached graph. Concurrent queries can use different values safely.
The former index setting `hnsw_search_candidates` is no longer accepted.

## Read replicas after index construction

`build-index --read-replicas-settings PER_AZ:3` builds the index and then sets
`READ_REPLICAS_SETTINGS` on both `indexImplLevelTable` and
`indexImplPostingTable`. It works with `hnsw` and `vector_kmeans_tree`:

```sh
ydb -e "$YDB_ENDPOINT" -d "$YDB_DATABASE" workload vector build-index \
    --table wikipedia --index idx_vector_cover --index-type hnsw \
    --vector-type float --vector-dimension 200 --distance inner_product \
    --kmeans-tree-levels 1 --kmeans-tree-clusters 10 \
    --read-replicas-settings PER_AZ:3
```

Supported formats are `PER_AZ:N` (N replicas per availability zone) and `ANY_AZ:N`
(N replicas across availability zones); zero disables replicas. Without the
option, no replica settings are changed. `--dry-run` prints the index creation
DDL followed by both ALTER statements. Invalid settings are rejected before
index creation, including use with `--index-type None`.

The server must allow alterations of index implementation tables. Index creation
and the two ALTER statements are separate operations: an ALTER failure returns
an error but does not roll back the index or a preceding successful ALTER.
The main table is not altered. Use `run select --stale-ro` to request stale reads
that can use replicas; provisioning replicas may take time after ALTER succeeds.
