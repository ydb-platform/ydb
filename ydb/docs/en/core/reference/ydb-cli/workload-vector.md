# Vector workload

This workload type is suited for testing performance and completeness of exact and approximate [vector search](../../concepts/vector_search.md).

Testing global and prefixed [vector indexes](../../yql/reference/syntax/create_table/vector_index.md) is supported. Search settings are determined automatically from the specified table and index.

## Initializing the test environment

### Loading the data

A vector dataset loaded into a table with a vector column and a vector index is required to run the test.

You can see vector index creation examples on the documentation page [Vector indexes](../../dev/vector-indexes.md#types).

### Preparing a test sample from a large table

{% note warning %}

This step is only required if you don't already have a test dataset and if the primary key of the main table isn't numeric, because auto-generation of the test set doesn't work for non-numeric keys.

{% endnote %}

In this case you may prepare a test set by selecting random rows from the main table manually. Example query to create the table for test samples:

```yql
CREATE TABLE `vector_index_sample` (
    id uint64 not null,
    prefix uint64 not null,
    embedding string not null,
    PRIMARY KEY (id)
);
```

A query to fill it with approximately 1000 rows from a large table:

```yql
INSERT INTO vector_index_sample
SELECT id, prefix, embedding FROM large_table
WHERE RandomNumber(id) < 0xFFFFFFFFFFFFFFFF / <number_of_rows_in_table> * 1000;
```

You can find the approximate number of rows in the table in table statistics without running `SELECT COUNT(*)`.

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
`--recall`                 | Measure recall metrics.                           | no
`--recall-threads`         | Number of concurrent queries during recall measurement. | `10`
`--non-indexed`            | Take vector settings from the index, but search without the index. | no

{% note warning %}

Pay attention to the parameter `--kmeans-tree-clusters` - its value significantly affects completeness and performance of the indexed vector search.

{% endnote %}

### Test run example

```bash
ydb -e grpc://hostname:2135 -d /Root/testdb workload vector run select --table wikipedia --index idx_vector --limit 20 \
    --query-table wikipedia_sample --limit 20 --targets 100 --kmeans-tree-clusters 10 --recall
```

### Detailed test description

- Workload generates SQL queries selecting up to `--limit` nearest rows according to vector distance from table `--table` with index `--index`.
- You don't need to specify column names (embedding, prefix, primary key columns) and distance function - they are determined automatically from the index definition.
- `--targets` vectors are used as a test set - either random ones from `--table` or first ones from `--query-table` in the primary key order.
- Random selection of test vectors from `--table` works only if its primary key is numeric.
- If `--query-table` is set, it should have the same vector and prefix column names as in `--table`.
- Search completeness measurement (`--recall`) is performed as a separate stage before the main performance test and shows the average rate of coincidence (from 0 to 1) of indexed search results with full-scan search results for all selected test vectors.
