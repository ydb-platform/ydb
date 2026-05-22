# Fulltext load

Allows you to test {{ ydb-short-name }} [fulltext search](../../concepts/query_execution/fulltext_search.md) performance using a document dataset. Supports both real datasets (e.g., [MS MARCO](https://microsoft.github.io/msmarco/TREC-Deep-Learning-2022)) and synthetically generated text based on a [Markov chain](https://en.wikipedia.org/wiki/Markov_chain) model.

The Markov model assumes that each next word depends on one or more previous words. The number of previous words on which the next word is based is called the model's order. First, we use an ordinary random generator with a uniform distribution to determine the text length, which we will then generate word by word using the Markov model until we reach the required number of words. Such data construction is necessary so that the text resembles some real human text rather than being just a random jumble of letters. A Markov model can be used both for generating the text of documents stored in a database and for generating select and upsert queries.


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
├─ clean              Drop tables created for load testing
└─ model              Build a Markov chain model from a text dataset
```

## Common command options {#common-options}

All commands support the following option:

| Name | Description | Default value |
|---|---|---|
| `--path` or `-p` | Path in the database where workload tables will be created. | `fulltext_workload` |

## Initializing the workload {#init}

Create the tables for the workload:

```bash
{{ ydb-cli }} workload fulltext init
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

After import is complete, a fulltext index named `index` is automatically built on the `text` column.

### Importing from files {#load-files}

Import documents from files (CSV, TSV, or Parquet, optionally gzip-compressed). The dataset must contain `id` and `text` columns. In the internal table, the `id` column is stored as `Uint64` and the `text` column as `String`.

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
 - To load an already built model:
     ```bash
     wget https://storage.yandexcloud.net/ydb-public/markov_dict.tsv.gz
     ```
 - To create your own model from Wikipedia data:
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
{{ ydb-cli }} workload fulltext import generator
```

#### Available options {#load-generator-options}

| Name | Description | Default value |
|---|---|---|
| `--model <path>` or `-m <path>` | Path to the Markov chain model file (`.tsv.gz`). | Required |
| `--rows <value>` | Number of rows to generate. | `100000` |
| `--min-sentence-len <value>` | Minimum number of words in a generated document. | `100` |
| `--max-sentence-len <value>` | Maximum number of words in a generated document. | `1000` |

We use parameters `--min-sentence-len` and `--max-sentence-len` to generate target text length with uniform distribution.

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
| `--order <value>` or `-n <value>` | Order of the Markov chain (n-gram context size). Order 1 uses only one word to predict next word, order 2 uses context from two words, etc. Must be between 1 and 5. | `1` |

The `--order` parameter specifies the number of previous words used to predict the next word. Typical values range from 1 to 5: lower values produce noisier text, while higher values generate text that is more similar to the source. The size of the Markov model grows exponentially with the `--order` value.

## Cleaning up {#cleanup}

Drop tables created for load testing:

```bash
{{ ydb-cli }} workload fulltext clean
```

## Usage examples {#examples}

### Example with a generated dataset

1. Download or train Markov chain model:
   - Download the model from S3:
     ```bash
     wget https://storage.yandexcloud.net/ydb-public/markov_dict.tsv.gz
     ```
   - Train the model from Wikipedia dataset:
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
    {{ ydb-cli }} workload fulltext import generator --model markov_dict.tsv.gz
    ```

4. Run the search workload:

    ```bash
    {{ ydb-cli }} workload fulltext run select --model markov_dict.tsv.gz
    ```
    Output example:
    ```
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1            23 23      0       0       242     407     453     453
    2            18 18      0       1       611     807     915     915
    3             6 6       0       7       161     991     991     991
    4            35 35      0       0       173     803     923     923
    5            59 59      0       1       135     647     759     803
    6            26 26      0       0       257     335     339     339
    7            15 15      0       1       651     963     975     975
    8            11 11      0       1       871     943     963     963
    9            18 18      0       6       190     967     999     999
    10           31 31      0       1       433     755     995     995

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10          242 24.2    0       18      259     915     991     999
    ```

    Column description:
     - Window – Sequential number of the time window (e.g., each second or fixed interval).
     - Txs – Number of transactions successfully completed in this window (or total across all windows in the bottom section).
     - Txs/Sec – Transaction rate per second for the given window (or average for total).
     - Retries – Number of automatic retries performed due to temporary errors (e.g., conflicts or throttling).
     - Errors – Number of unrecoverable errors encountered.
     - p50(ms) – Median (50th percentile) transaction latency in milliseconds.
     - p95(ms) – 95th percentile latency (milliseconds).
     - p99(ms) – 99th percentile latency (milliseconds).
     - pMax(ms) – Maximum observed latency within the window (or overall maximum for total).

5. Run the upsert workload:

    ```bash
    {{ ydb-cli }} workload fulltext run upsert --model markov_dict.tsv.gz
    ```
    Output example:
    ```
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1           255 255     0       0       31      34      117     123
    2           279 279     0       0       32      37      42      45
    3           282 282     0       0       31      35      37      40
    4           277 277     0       0       32      37      39      42
    5           281 281     0       0       32      38      40      42
    6           278 278     0       0       32      39      41      43
    7           279 279     0       0       32      38      39      43
    8           275 275     0       0       33      39      42      46
    9           282 282     0       0       32      37      39      40
    10          293 293     0       0       31      37      39      41

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10         2781 278.1   0       0       32      38      41      123
    ```
6. Clean up:

    ```bash
    {{ ydb-cli }} workload fulltext clean
    ```

### Example with the MS MARCO dataset

1. Download the quality bundle (contains `documents.tsv.gz`, `queries.tsv.gz`, `markov_dict.tsv.gz`, and `query_relevances.tsv.gz`):

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
    {{ ydb-cli }} workload fulltext run select --quality
    ```
    Output example:
    ```
    Search quality measurement...
    Search quality measurement completed for 100 queries in 1 seconds.
    nDCG@10:  0.270
    Errors:   0
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1            23 23      0       0       242     407     453     453
    2            18 18      0       1       611     807     915     915
    3             6 6       0       7       161     991     991     991
    4            35 35      0       0       173     803     923     923
    5            59 59      0       1       135     647     759     803
    6            26 26      0       0       257     335     339     339
    7            15 15      0       1       651     963     975     975
    8            11 11      0       1       871     943     963     963
    9            18 18      0       6       190     967     999     999
    10           31 31      0       1       433     755     995     995

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10          242 24.2    0       18      259     915     991     999
    ```
5. Run the upsert workload:

    ```bash
    {{ ydb-cli }} workload fulltext run upsert
    ```
    Output example:
    ```
    Window      Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    1           255 255     0       0       31      34      117     123
    2           279 279     0       0       32      37      42      45
    3           282 282     0       0       31      35      37      40
    4           277 277     0       0       32      37      39      42
    5           281 281     0       0       32      38      40      42
    6           278 278     0       0       32      39      41      43
    7           279 279     0       0       32      38      39      43
    8           275 275     0       0       33      39      42      46
    9           282 282     0       0       32      37      39      40
    10          293 293     0       0       31      37      39      41

    Total       Txs Txs/Sec Retries Errors  p50(ms) p95(ms) p99(ms) pMax(ms)
    10         2781 278.1   0       0       32      38      41      123
    ```
6. Clean up:

    ```bash
    {{ ydb-cli }} workload fulltext clean
    ```
