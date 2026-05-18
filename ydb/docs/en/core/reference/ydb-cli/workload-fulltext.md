# Fulltext workload

Allows you to test {{ ydb-short-name }} [fulltext search](../../concepts/query_execution/fulltext_search.md) performance using a document dataset. Supports both real datasets (e.g., MS MARCO) and synthetically generated text based on a Markov chain model.

## Command structure {#structure}

```bash
{{ ydb-cli }} [global options...] workload fulltext [options...] <subcommand>
```

Subcommands:

```
fulltext            YDB fulltext workload
├─ init               Initialize tables for the workload
├─ import             Load data and build a fulltext index
│   ├─ files            Import data from files
│   └─ generator        Generate random text using a Markov chain model
├─ run                Run the workload
│   ├─ select           Search documents using fulltext queries
│   └─ upsert           Insert or update documents in the table
├─ clean              Drop tables created during initialization
└─ model              Build a Markov chain model from a text dataset
```

## Common command options {#common-options}

All commands support the following option:

| Name | Description | Default value |
|---|---|---|
| `--path` or `-p` | Path in the database where workload tables will be created. | `fulltext_workload` |

## Initializing the workload {#init}

Create the table for the workload:

```bash
{{ ydb-cli }} workload fulltext --path fulltext init
```

### Available options {#init-options}

| Name | Description | Default value |
|---|---|---|
| `--min-partitions <value>` | Minimum number of table partitions. | `40` |
| `--partition-size <value>` | Target partition size, in MB. | `2000` |
| `--auto-partition <value>` | Enable auto-partitioning by load (`1` — enabled, `0` — disabled). | `1` |
| `--clear` | Drop and recreate the table if it already exists. | |

## Loading data {#load}

After initialization, load data into the table and build the fulltext index. There are two subcommands: `files` to import from an existing dataset and `generator` to generate synthetic data.

After import is complete, a fulltext index is automatically built on the `text` column.

### Importing from files {#load-files}

Import documents from files (CSV, TSV, or Parquet, optionally gzip-compressed). The dataset must contain `id` and `text` columns.

Example:

```bash
{{ ydb-cli }} workload fulltext import files
```

#### Available options {#load-files-options}

| Name | Description | Default value |
|---|---|---|
| `--input <path>` or `-i <path>` | Path to the dataset file or directory. Supported formats: CSV/TSV (optionally gzip-compressed), Parquet. Only `id` and `text` columns are imported. | Required |

{% include [load_options](./_includes/workload/load_options.md) %}

### Generating synthetic data {#load-generator}

Generate random text data using a Markov chain model and load it into the table. You must first [build the model](#model) or download a pre-built one.

```bash
{{ ydb-cli }} workload fulltext import generator
```

#### Available options {#load-generator-options}

| Name | Description | Default value |
|---|---|---|
| `--model <path>` or `-m <path>` | Path to the Markov chain model file (`.tsv.gz`). | Required |
| `--rows <value>` | Number of rows to generate. | `100000` |
| `--min-sentence-len <value>` | Minimum number of words in a generated document. | `100` |
| `--max-sentence-len <value>` | Maximum number of words in a generated document. | `1000` |

{% include [load_options](./_includes/workload/load_options.md) %}

## Running the workload {#run}

Run load testing using one of two modes: `select` (fulltext search queries) or `upsert` (inserting new documents).

### Search workload {#run-select}

Executes fulltext search queries against the indexed table. Queries can be generated from a Markov chain model or read from a pre-loaded query table.

```bash
{{ ydb-cli }} workload fulltext run select --model markov_dict.tsv.gz
```

#### Available options {#run-select-options}

| Name | Description | Default value |
|---|---|---|
| `--model <path>` or `-m <path>` | Path to the Markov chain model file (`.tsv.gz`) for generating queries. Either `--model` or `--query-table` must be specified. | |
| `--query-table <name>` | Name of the table containing pre-loaded queries. The table must have a `query` column. Either `--model` or `--query-table` must be specified. | |
| `--index-name <name>` | Name of the fulltext index to use. | `index` |
| `--min-query-len <value>` | Minimum number of words in a generated query. | `1` |
| `--max-query-len <value>` | Maximum number of words in a generated query. | `5` |
| `--top-size <value>` | Number of rows to sample from the table to build the query word set. | `1000` |
| `--limit <value>` | Limit the number of results returned per query. `0` means no limit. | `0` |

{% include [run_options](./_includes/workload/run_options.md) %}

### Upsert workload {#run-upsert}

Continuously inserts new documents into the table using a Markov chain model to generate text.

```bash
{{ ydb-cli }} workload fulltext run upsert --model markov_dict.tsv.gz
```

#### Available options {#run-upsert-options}

| Name | Description | Default value |
|---|---|---|
| `--model <path>` or `-m <path>` | Path to the Markov chain model file (`.tsv.gz`). | Required |
| `--index-name <name>` | Name of the fulltext index to use. | `index` |
| `--bulk-size <value>` | Number of rows per upsert batch. | `100` |
| `--min-sentence-len <value>` | Minimum number of words in a generated document. | `100` |
| `--max-sentence-len <value>` | Maximum number of words in a generated document. | `1000` |

{% include [run_options](./_includes/workload/run_options.md) %}

## Building a Markov chain model {#model}

Before using the generator or the `run upsert` / `run select` modes with generated queries, you need to build a Markov chain model from a text dataset. The model captures word transition probabilities and is used to generate realistic text.

```bash
{{ ydb-cli }} workload fulltext model --input wikipedia_sample.csv.gz --output markov_dict.tsv.gz --order 3
```

### Available options {#model-options}

| Name | Description | Default value |
|---|---|---|
| `--input <path>` or `-i <path>` | Path to the dataset file or directory. Supports `.csv[.gz]` and `.tsv[.gz]` formats. The file must have a `text` column. | Required |
| `--output <path>` or `-o <path>` | Output file path for the model dictionary. | `markov_dict.tsv.gz` |
| `--order <value>` or `-n <value>` | Order of the Markov chain (n-gram context size). Order 1 uses unigram context, order 2 uses bigram context, etc. Must be between 1 and 5. | `1` |

## Cleaning up {#cleanup}

Drop all tables created during initialization:

```bash
{{ ydb-cli }} workload fulltext clean
```

The command has no additional parameters.

## Usage examples {#examples}

### Example with a generated dataset

1a. Download a Markov chain model from s3:

    ```bash
    wget https://storage.yandexcloud.net/ydb-public/markov_dict.tsv.gz
    ```

1b. Build a Markov chain model from a Wikipedia sample:
    ```python
    from datasets import load_dataset

    ds = load_dataset(
       "rumbleFTW/wikipedia-20220301-en-raw",
       split="train[:1000000]",
       streaming=False,
       )
    ds.to_csv('wikipedia_sample.csv.gz', compression='gzip', index=False)
    ```

    ```bash
    {{ ydb-cli }} workload fulltext model --input wikipedia_sample.csv.gz --output markov_dict.tsv.gz --order 3
    ```

2. Initialize the workload table:

    ```bash
    {{ ydb-cli }} workload fulltext init
    ```

3. Generate and load synthetic documents:

    ```bash
    {{ ydb-cli }} workload fulltext import generator
    ```

4. Run the search workload:

    ```bash
    {{ ydb-cli }} workload fulltext run select
    ```

5. Run the upsert workload:

    ```bash
    {{ ydb-cli }} workload fulltext run upsert
    ```

6. Clean up:

    ```bash
    {{ ydb-cli }} workload fulltext clean
    ```

### Example with the MS MARCO dataset

1. Download the quality bundle (contains `documents.tsv.gz`, `queries.tsv.gz`, `markov_dict.tsv.gz`, and 'query_relevances.tsv.gz'):

    ```bash
    wget https://storage.yandexcloud.net/ydb-public/quality-bundle.tar
    tar -xf quality-bundle.tar
    ```

2. Initialize the workload table:

    ```bash
    {{ ydb-cli }} workload fulltext init
    ```

3. Import documents from the dataset:

    ```bash
    {{ ydb-cli }} workload fulltext import files
    ```

4. Run the search workload using the pre-built queries:

    ```bash
    {{ ydb-cli }} workload fulltext run select
    ```

5. Clean up:

    ```bash
    {{ ydb-cli }} workload fulltext clean
    ```
