# Vector Workload

This workload type is suited for testing the performance and completeness of exact and approximate [vector search](../../concepts/vector_search.md).

Testing of global and prefixed [vector indexes](../../yql/reference/syntax/create_table/vector_index.md) is supported.

The test runs vector search queries and evaluates their performance and completeness. You can choose different indices and search parameters to understand their effect on the test results.

## Test Algorithm

1. A test set is generated:
   - If a table with a test set is specified in the command line, the first `targets` entries in primary key order are selected from it.
   - Otherwise, `targets` random entries are selected from the main table.
2. Completeness measurement is performed:
   - Two queries are executed for each item in the test set.
   - The first query performs an [exact vector search](../../concepts/vector_search.md#vector-search-exact) (full scan) based on vector distance, forming the `R_exact` result set.
   - The second query performs an [approximate vector search](../../concepts/vector_search.md#vector-search-index) using the vector index, forming the `R_approx` result set.
   - If a prefix index is selected, both queries scan only the entries with prefixes equal to the prefix of the test set entry.
   - Completeness of the approximate search is calculated using the formula $\frac{|R_{approx} \bigcap R_{exact}|}{R_{exact}}$ (here `|A|` is the number of elements in the set A and `A ∩ B` is the intersection of sets A and B).
3. Performance measurement is performed:
   - Indexed search queries are run for a specified duration and with a specified number of parallel threads.
   - Each query is executed for a random entry from the test set.
   - The time limit and parallelism are specified in the [global options](commands/workload/index.md#global_workload_options).
   - The average number of requests per second (RPS) and response time percentiles are calculated.

## Initializing the Test Environment

### Loading the Data

A vector dataset loaded into a table with a vector column and a vector index is required to run the test.

A test set of vectors is also required. You can either put it in a separate table or generate it randomly from the main table.

You can see vector index creation examples on the documentation page [Vector Indexes](../../dev/vector-indexes.md#types).

### Preparing a Test Sample from a Large Table

{% note warning %}

This step is only required if you don't already have a test dataset and if the primary key of the main table isn't numeric, because auto-generation of the test set doesn't work for non-numeric keys.

{% endnote %}

In this case, you may prepare a test set by selecting random rows from the main table manually. Example query to create the table for test samples:

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

## Running the test {#run-select}

Command syntax:

```bash
{{ ydb-cli }} [global options...] workload vector run select [global workload options...] [options...]
```

* `global workload options`: [Global options for all types of load](commands/workload/index.md#global_workload_options).
* `global options`: [Global parameters](commands/global-options.md).
* `options`: Command parameters.

### Test options

View the description of the command:

```bash
{{ ydb-cli }} workload vector run select --help
```

Command parameters:

Parameter name | Parameter description | Default value
---|---|---
`--table`                  | Table name.                                       | `vector_index_workload`
`--index`                  | Index name.                                       | `index`
`--query-table`            | Name of the table with predefined search vectors. | empty
`--targets`                | Number of vectors to search as targets.           | `100`
`--limit`                  | Maximum number of vectors to return.              | `5`
`--kmeans-tree-clusters`   | Maximum number of clusters to use during search ([KMeansTreeSearchTopSize](../../yql/reference/syntax/select/vector_index.md#kmeanstreesearchtopsize)). | `1`
`--recall`                 | Measure [recall](https://en.wikipedia.org/wiki/Precision_and_recall) metrics.                           | no
`--recall-threads`         | Number of concurrent queries during recall measurement. | `10`
`--non-indexed`            | Take vector settings from the index, but search without the index. | no

{% note warning %}

Pay attention to the `--kmeans-tree-clusters` parameter as raising it significantly increases completeness of the search at the expense of slowing it down. You can try values from 1 to the number of clusters specified when creating the index.

{% endnote %}

### Test run example

```bash
{{ ydb-cli }} -e grpc://hostname:2135 -d /Root/testdb workload vector run select --table wikipedia --index idx_vector --limit 20 \
    --query-table wikipedia_sample --limit 20 --targets 100 --kmeans-tree-clusters 10 --recall
```

### Remarks

- The workload generates SQL queries selecting up to `limit` nearest rows according to vector distance from table `table` with index `index`.
- You don't need to specify column names (embedding, prefix, primary key columns) or the distance function because they are determined automatically from the index definition.
- `targets` vectors are used as a test set. The workload either selects random rows from `table` or the first rows from `query-table` in primary key order.
- Random selection of test vectors from `table` works only if its primary key is numeric.
- If `query-table` is set, it must have the same vector and prefix column names as in `table`.
- Search completeness measurement (`recall`) is performed as a separate stage before the primary performance test and shows the average rate of coincidence (from 0 to 1) of indexed search results with full-scan search results for all selected test vectors.
