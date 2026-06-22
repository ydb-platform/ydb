# Vector load

Allows you to test {{ ydb-short-name }} [vector search](../../concepts/query_execution/vector_search.md) performance and recall using exact and approximate search. Supports both global and [filtered vector indexes](../../yql/reference/syntax/create_table/vector_index.md).

The workload supports importing vectors from a real dataset (e.g., [Wikipedia embeddings](https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings)) or generating synthetic random vectors. After loading data, you can build a vector index, run search queries to measure recall and performance, and clean up the workload tables.

## Command structure {#structure}

```bash
{{ ydb-cli }} [global options...] workload vector [options...] <subcommand>
```

Subcommands:

```text
vector                YDB vector workload.
├─ init                 Create and initialize tables for the workload
├─ import               Fill workload tables with data
│   ├─ files              Import vectors from files
│   └─ generator          Generate random vectors
├─ build-index          Create and initialize a vector index for the workload
├─ drop-index           Drop the vector index created for the workload
├─ run                  Run YDB vector workload
│   ├─ select            Search vectors and measure performance/recall
│   └─ upsert            Insert or update vector rows in the table
└─ clean                Drop tables created for load testing
```

## Initializing the workload {#init}

Create the table for the workload:

```bash
{{ ydb-cli }} workload vector init
```

### Available options {#init-options}

| Name | Description | Default value |
|---|---|---|
| `--table <name>` | Name of the main table that stores vectors. | `vector_index_workload` |
| `--min-partitions <value>` | Minimum number of table partitions. | `40` |
| `--partition-size <value>` | Target partition size, in MB. | `2000` |
| `--auto-partition <value>` | Enable auto-partitioning by load (`1` — enabled, `0` — disabled). | `1` |
| `--prefixed` | Add a `prefix` column to the table for use with a [filtered (prefixed) vector index](../../yql/reference/syntax/create_table/vector_index.md). | |
| `--clear` | Drop and recreate the table if it already exists. | |
| `--dry-run` | Print the DDL query instead of executing it. | |

The created table has the following schema:

- `id Uint64 NOT NULL` — primary key;
- `embedding String` — serialized embedding vector;
- `prefix Uint64 NOT NULL` (only when `--prefixed` is specified) — filter column for prefixed indexes.

## Loading data {#load}

After initialization, load data into the table. There are two subcommands: `files` to import vectors from an existing dataset and `generator` to generate synthetic random vectors.

After import is complete, a vector index named `index` is automatically built on the `embedding` column (unless `--index-type None` is specified).

### Importing from files {#load-files}

Import vectors from files (CSV, TSV, or Parquet, optionally gzip-compressed). The dataset must contain `id` and `embedding` columns. Any additional columns are ignored.

For CSV/TSV files, embeddings must be encoded as a list of floats, e.g., `"[ 1.0, 2.0, 3.0 ]"`. For Parquet files, embeddings can be a list of `float32` values or already serialized YDB binary embeddings.

Example:

```bash
{{ ydb-cli }} workload vector import files
```

#### Available options {#load-files-options}

| Name | Description | Default value |
|---|---|---|
| `--input <path>` or `-i <path>` | Path to the dataset file or directory. Supported formats: CSV/TSV (optionally gzip-compressed), Parquet. Only `id` and `embedding` columns are imported. | Required |
| `--format <format>` | Source files format. One of `csv`, `tsv`, `parquet`. When set, only files matching the specified format are imported from a directory. When not set, format is auto-detected from file extensions. | |
| `--embedding-column-name <name>` | Alternative source column name for the embedding field in input files. | `embedding` |
| `--table <name>` | Name of the table to load data into. | `vector_index_workload` |
| `--index <name>` | Name of the vector index to build after import. | `index` |
| `--index-type <type>` | Index type to build after import. Possible values: `None`, `KmeansTree`. Set to `None` to skip index building. | `KmeansTree` |
| `--vector-type <type>` | Type of vectors. One of `float`, `int8`, `uint8`, `bit`. | `float` |
| `--vector-dimension <value>` | Vector dimension (size of embedding vectors). | `1024` |
| `--distance <value>` | Distance or similarity function. One of `inner_product`, `cosine`, `euclidean`, `manhattan`. | `inner_product` |
| `--kmeans-tree-levels <value>` | Number of levels in the kmeans tree. See [kmeans-tree type](../../dev/vector-indexes.md#kmeans-tree-type). | `1` |
| `--kmeans-tree-clusters <value>` | Number of clusters in kmeans. See [kmeans-tree type](../../dev/vector-indexes.md#kmeans-tree-type). | `10` |
| `--kmeans-tree-covering <value>` | Build a covering index (`1` — enabled, `0` — disabled). | `0` |
| `--kmeans-tree-prefixed <value>` | Build a prefixed (filtered) index (`1` — enabled, `0` — disabled). The table must have been created with the `--prefixed` option. | `0` |

{% note info %}

For more details on `--kmeans-tree-*` index building parameters, see [kmeans-tree type](../../dev/vector-indexes.md#kmeans-tree-type).

{% endnote %}

{% include [load_options](./_includes/workload/load_options.md) %}

### Generating synthetic data {#load-generator}

Generate random vectors and load them into the table. Vector components are sampled from a uniform distribution and serialized into YDB binary embedding format.

```bash
{{ ydb-cli }} workload vector import generator
```

#### Available options {#load-generator-options}

| Name | Description | Default value |
|---|---|---|
| `--rows <value>` | Number of rows to generate. | `10000` |
| `--prefix-count <value>` | Number of distinct prefix values for a prefixed index. | `100` |
| `--seed <value>` | Seed for the random number generator. | `42` |
| `--table <name>` | Name of the table to load data into. | `vector_index_workload` |
| `--index <name>` | Name of the vector index to build after import. | `index` |
| `--index-type <type>` | Index type to build after import. Possible values: `None`, `KmeansTree`. | `KmeansTree` |
| `--vector-type <type>` | Type of vectors. One of `float`, `int8`, `uint8`, `bit`. | `float` |
| `--vector-dimension <value>` | Vector dimension. | `1024` |
| `--distance <value>` | Distance or similarity function. | `inner_product` |
| `--kmeans-tree-levels <value>` | Number of levels in the kmeans tree. | `1` |
| `--kmeans-tree-clusters <value>` | Number of clusters in kmeans. | `10` |
| `--kmeans-tree-covering <value>` | Build a covering index. | `0` |
| `--kmeans-tree-prefixed <value>` | Build a prefixed (filtered) index. | `0` |

{% note info %}

For more details on `--kmeans-tree-*` index building parameters, see [kmeans-tree type](../../dev/vector-indexes.md#kmeans-tree-type).

{% endnote %}

{% include [load_options](./_includes/workload/load_options.md) %}

## Building a vector index {#build-index}

If the table was loaded with `--index-type None`, or if you want to build an additional index with different parameters, you can build a vector index on an existing table using the `build-index` command.

```bash
{{ ydb-cli }} workload vector build-index --distance cosine
```

### Available options {#build-index-options}

| Name | Description | Default value |
|---|---|---|
| `--table <name>` | Name of the table to build the index on. | `vector_index_workload` |
| `--index <name>` | Name of the index to create. | `index` |
| `--vector-type <type>` | Type of vectors. One of `float`, `int8`, `uint8`, `bit`. | `float` |
| `--vector-dimension <value>` | Vector dimension. | `1024` |
| `--distance <value>` | Distance or similarity function. | `inner_product` |
| `--kmeans-tree-levels <value>` | Number of levels in the kmeans tree. | `1` |
| `--kmeans-tree-clusters <value>` | Number of clusters in kmeans. | `10` |
| `--dry-run` | Print the DDL query instead of executing it. | |

{% note info %}

For more details on `--kmeans-tree-*` index building parameters, see [kmeans-tree type](../../dev/vector-indexes.md#kmeans-tree-type).

{% endnote %}

## Dropping a vector index {#drop-index}

Drop a previously built vector index.

```bash
{{ ydb-cli }} workload vector drop-index
```

### Available options {#drop-index-options}

| Name | Description | Default value |
|---|---|---|
| `--table <name>` | Name of the table that holds the index. | `vector_index_workload` |
| `--index <name>` | Name of the index to drop. | `index` |
| `--dry-run` | Print the DDL query instead of executing it. | |

## Running the workload {#run}

Run load testing using one of two modes: `select` (vector search queries) or `upsert` (inserting new vector rows).

### Search workload {#run-select}

Executes vector search queries against the indexed table. Optionally measures recall by comparing approximate search results with exact (full-scan) results, then runs a performance benchmark.

```bash
{{ ydb-cli }} workload vector run select --recall
```

#### Available options {#run-select-options}

| Name | Description | Default value |
|---|---|---|
| `--table <name>` | Name of the main table with vectors. | `vector_index_workload` |
| `--index <name>` | Name of the vector index to use. | `index` |
| `--query-table <name>` | Name of the table with predefined search vectors. If not specified, random rows from the main table are used. | |
| `--targets <value>` | Number of vectors to use as test targets. | `100` |
| `--limit <value>` | Maximum number of nearest vectors to return per query. | `5` |
| `--kmeans-tree-clusters <value>` | Maximum number of clusters to inspect during search ([KMeansTreeSearchTopSize](../../yql/reference/syntax/select/vector_index.md#kmeanstreesearchtopsize)). | `1` |
| `--recall` | Measure [recall](https://en.wikipedia.org/wiki/Precision_and_recall) of the approximate search compared to exact search. | |
| `--recall-threads <value>` | Number of concurrent queries during recall measurement. | `10` |
| `--non-indexed` | Take vector settings from the index, but search without the index (full scan). | |
| `--stale-ro` | Read with StaleRO consistency mode. | |

{% note warning %}

Pay attention to the `--kmeans-tree-clusters` parameter — increasing it significantly improves search recall at the expense of speed. Try values from 1 to the number of clusters specified when creating the index.

{% endnote %}

{% include [run_options](./_includes/workload/run_options.md) %}

### Upsert workload {#run-upsert}

Continuously inserts new vector rows into the table, generating random embeddings on the fly.

```bash
{{ ydb-cli }} workload vector run upsert
```

#### Available options {#run-upsert-options}

| Name | Description | Default value |
|---|---|---|
| `--table <name>` | Name of the table to insert into. | `vector_index_workload` |
| `--index <name>` | Name of the vector index. | `index` |
| `--bulk-size <value>` | Number of rows per upsert batch. | `100` |
| `--prefixed` | Generate upserts with a `prefix` column (for prefixed indexes). | |
| `--prefix-count <value>` | Number of distinct prefix values. Used only when `--prefixed` is set. | `1000` |

{% include [run_options](./_includes/workload/run_options.md) %}

## Cleaning up {#cleanup}

Drop tables created for load testing:

```bash
{{ ydb-cli }} workload vector clean
```

## Test algorithm {#algorithm}

The `run select` mode performs the following stages:

1. A test set is generated:
   - If a table with a test set is specified via `--query-table`, the first `--targets` rows are selected from it in primary key order.
   - Otherwise, `--targets` random rows are selected from the main table.
2. Recall measurement is performed (if `--recall` is specified):
   - Two queries are executed for each item in the test set.
   - The first query performs an [exact vector search](../../concepts/query_execution/vector_search.md#vector-search-exact) (full scan) based on vector distance, forming the `R_exact` result set.
   - The second query performs an [approximate vector search](../../concepts/query_execution/vector_search.md#vector-search-index) using the vector index, forming the `R_approx` result set.
   - If a filtered vector index is selected, both queries scan only the entries with matching filter column values.
   - Recall of the approximate search is calculated using the formula $\frac{|R_{approx} \bigcap R_{exact}|}{|R_{exact}|}$ (where `|A|` is the number of elements in the set A and `A ∩ B` is the intersection of sets A and B).
3. Performance measurement is performed:
   - Indexed search queries are run for a specified duration and with a specified number of parallel threads.
   - Each query is executed for a random entry from the test set.
   - The average number of requests per second (RPS) and response time percentiles are calculated.

## Usage examples {#examples}

### Example with generated data

1. Initialize the workload table:

    ```bash
    {{ ydb-cli }} workload vector init
    ```

2. Generate and load synthetic vectors:

    ```bash
    {{ ydb-cli }} workload vector import generator --rows 100000 --distance cosine
    ```

3. Run the search workload:

    ```bash
    {{ ydb-cli }} workload vector run select --recall
    ```

    Output example:

    ```text
    Recall: 0.8950
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1            100 100     0       0       5       8       12      15
    2             98 98      0       0       5       9       13      16
    ...

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10          980 98.0    0       0       5       9       14      18
    ```

    Column descriptions:
     - `Window` — Sequential number of the time window (e.g., each second or fixed interval).
     - `Txs` — Number of transactions successfully completed in this window (or total across all windows in the bottom section).
     - `Txs/Sec` — Transaction rate per second for the given window (or average for total).
     - `Retries` — Number of automatic retries performed due to temporary errors (e.g., conflicts or throttling).
     - `Errors` — Number of unrecoverable errors encountered.
     - `p50(ms)` — Median (50th percentile) transaction latency in milliseconds.
     - `p95(ms)` — 95th percentile latency (milliseconds).
     - `p99(ms)` — 99th percentile latency (milliseconds).
     - `pMax(ms)` — Maximum observed latency within the window (or overall maximum for total).

4. Run the upsert workload:

    ```bash
    {{ ydb-cli }} workload vector run upsert
    ```

5. Clean up:

    ```bash
    {{ ydb-cli }} workload vector clean
    ```

### Example with an external dataset

1. Prepare tables and load data. See the recipe [Vector index with external dataset loading](../../recipes/vector-search/vector-index-with-prepared-dataset.md).

    Examples of creating vector indexes are available on the [Vector Indexes](../../dev/vector-indexes.md#types) documentation page.

2. Run the search workload:

    ```bash
    {{ ydb-cli }} -e grpc://hostname:2135 -d /Root/testdb workload vector run select \
        --table wikipedia --index idx_vector \
        --query-table wikipedia_sample --recall
    ```

    Output example:

    ```text
    Recall: 0.8950
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1            100 100     0       0       5       8       12      15
    2             98 98      0       0       5       9       13      16
    ...

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10          980 98.0    0       0       5       9       14      18
    ```

## Remarks

- The search workload generates SQL queries that select nearest rows by vector distance from table `--table` using index `--index`.
- You don't need to specify column names (embedding, filter, primary key columns) or the distance function — they are automatically extracted from the index definition.
- Random selection of test vectors from `--table` works only if its primary key is numeric.
- If `--query-table` is specified, it must have the same vector and filter column names as `--table`.
- Recall measurement (`--recall`) is performed as a separate stage before the main performance test and shows the average overlap ratio (from 0 to 1) of indexed search results with full-scan search results across all selected test vectors.

### Preparing a test sample from a large table {#sample-prep}

{% note warning %}

This step is only required if you don't already have a test dataset and if the primary key of the main table isn't numeric, because auto-generation of the test set doesn't work for non-numeric keys.

{% endnote %}

In this case, you can prepare a test set by selecting random rows from the main table manually. Example query to create the table for test samples:

```yql
CREATE TABLE vector_index_sample (
    id Uint64 NOT NULL,
    prefix Uint64 NOT NULL,
    embedding String NOT NULL,
    PRIMARY KEY (id)
);
```

A query to fill it with approximately 1000 rows from a large table:

```yql
INSERT INTO vector_index_sample
SELECT id, prefix, embedding FROM large_table
WHERE RandomNumber(id) < 0xFFFFFFFFFFFFFFFF / <number_of_rows_in_table> * 1000;
```

{% note tip %}

You can find the approximate number of rows in the table in [table statistics](commands/scheme-describe.md) without running `SELECT COUNT(*)`.

{% endnote %}
