# Vector index with a prepared external dataset

This article walks you through working with vector indexes in {{ ydb-short-name }}. It uses an English Wikipedia text dataset (485,859 rows) prepared by the [Hugging Face](https://huggingface.co) community.

The article covers:

* [creating a table for loading data](#step1);
* [downloading the dataset](#step2);
* [importing data into the table](#step3);
* [building the vector index](#step4);
* [running vector search without the index](#step5);
* [running vector search with the index](#step6).

## Prerequisites

To run the examples in this article you need:

* A {{ ydb-short-name }} database with [vector index](../../dev/vector-indexes.md) support enabled. For a simple single-node cluster setup, see [here](../../quickstart.md). For production deployment options, see [here](../../devops/deployment-options/index.md).

* On your machine:
    * The [{{ ydb-short-name }} CLI](../../reference/ydb-cli/);
    * Python 3;

* Network access from your machine to the hosts where {{ ydb-short-name }} is running.

## Step 1. Creating a table {#step1}

First, create a table in {{ ydb-short-name }} to store the data. You can do this with an SQL query:

```yql
CREATE TABLE wikipedia (
  id Uint64 NOT NULL,
  title Utf8,
  text Utf8,
  url Utf8,
  wiki_id Uint32,
  views Float,
  paragraph_id Uint32,
  langs Uint32,
  emb Utf8,
  embedding String,
  PRIMARY KEY (id)
);
```

## Step 2. Downloading the dataset {#step2}

Download the [dataset](https://huggingface.co/datasets/Cohere/wikipedia-22-12-simple-embeddings) from [Hugging Face](https://huggingface.co). It contains English Wikipedia texts split into paragraphs, with an embedding vector for each paragraph.

In your working directory, create a file `import_dataset.py` with this Python script:

```python
#!/usr/bin/env python
from datasets import load_dataset

# Load the dataset
dataset = load_dataset("Cohere/wikipedia-22-12-simple-embeddings")

# Save it as CSV or another format locally
dataset['train'].to_csv('wikipedia_embeddings_train.csv', index=False)
```

Install the `datasets` package and run the script:

```bash
pip3 install datasets
python3 import_dataset.py
```

This creates `wikipedia_embeddings_train.csv` in your working directory with the Wikipedia texts and embeddings.

## Step 3. Importing data from file into the table {#step3}

Use the [{{ ydb-short-name }} CLI](../../reference/ydb-cli/) to import the file `wikipedia_embeddings_train.csv` into the table `wikipedia` created in [Step 1](#step1).

In the command below:

* `<endpoint>` — [endpoint](../../concepts/connect.md#endpoint) of your {{ ydb-short-name }} database
* `<database>` — [database path](../../concepts/connect.md#database) of your {{ ydb-short-name }} database

```bash
ydb -e <endpoint> -d <database> -v import file csv --path wikipedia --header wikipedia_embeddings_train.csv --timeout 30
```

In this example the test {{ ydb-short-name }} server runs without authentication, so no user credentials are passed to the CLI. For CLI authentication, see [here](../../reference/ydb-cli/connect.md).

The file `wikipedia_embeddings_train.csv` is about 6.26 GB, so the upload may take a while. When it finishes, you can verify that all rows were loaded into the `wikipedia` table from [Step 1](#step1) with:

```yql
SELECT COUNT(*) AS row_count FROM wikipedia
```

Query result:

```bash
row_count
485859
```

For {{ ydb-short-name }} vector indexes, vectors must be in string format (see [data types](../../yql/reference/udf/list/knn.md#data-types)). The embeddings in the imported dataset (stored in the `emb` column) can be converted into the `embedding` column in the required format by running this script on your machine:

```bash
for (( i=0; i <= 490000; i+=10000 ))
do
echo $i
echo "{\"begin\":$i}" | ydb -e <endpoint> -d <database> table query execute -q 'declare $begin As Int32; UPDATE wikipedia SET embedding = Unwrap(Untag(Knn::ToBinaryStringFloat(Cast(String::SplitToList(String::ReplaceAll(String::RemoveAll(String::RemoveFirst(String::RemoveLast(emb, "]"), "["), "\n"), "  ", " "), " ") AS List<Float>)), "FloatVector")) WHERE id>=$begin AND id < $begin + 10000;'
done
```

## Step 4. Building the vector index {#step4}

To create the vector index `idx_vector` on the `wikipedia` table, run:

```yql
ALTER TABLE wikipedia
ADD INDEX idx_vector
GLOBAL USING vector_kmeans_tree
ON (embedding)
WITH (
  distance=cosine,
  vector_type="float",
  vector_dimension=768,
  levels=1,
  clusters=200);
```

This creates a `vector_kmeans_tree` index. For more on this index type, see [here](../../dev/vector-indexes.md#kmeans-tree-type).

For general information on vector indexes, creation parameters, and current limitations, see [{#T}](../../dev/vector-indexes.md).

## Step 5. Search in the table without using the vector index {#step5}

This step runs exact search for the 3 nearest neighbors of a given vector **without** using the index. The query vector is assumed to be produced from the search text by an encoder model (e.g. [Embed from cohere.com](https://cohere.com/embed)).

First, the target vector is encoded to binary with [`Knn::ToBinaryStringFloat`](../../yql/reference/udf/list/knn.md#functions-convert).

Then cosine distance is computed from each row’s `embedding` to the target vector.

Rows are ordered by distance ascending, and the first three (`$K`) rows are returned as the nearest neighbors.

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat(Cast([0.1961289,0.51426697,...] AS List<Float>));

SELECT id, title, text, wiki_id, Knn::CosineDistance(embedding, $TargetEmbedding) as CosineDistance
FROM wikipedia VIEW PRIMARY KEY
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

For details on exact vector search without vector indexes, see the [Knn UDF](../../yql/reference/udf/list/knn.md) documentation.

## Step 6. Search in the table using the vector index {#step6}

To search for the 3 nearest neighbors of a given vector using the index `idx_vector` created in [Step 4](#step4), run:

```yql
$K = 3;
$TargetEmbedding = Knn::ToBinaryStringFloat(Cast([0.1961289,0.51426697,...] AS List<Float>));

SELECT id, title, text, wiki_id, Knn::CosineDistance(embedding, $TargetEmbedding) as CosineDistance
FROM wikipedia VIEW idx_vector
ORDER BY Knn::CosineDistance(embedding, $TargetEmbedding)
LIMIT $K;
```

The result when using the vector index can differ from the result without it.

Vector indexes provide **approximate** search and are optimized for speed. To improve performance, some data is pruned from consideration, so a few vectors that would have been among the nearest may be missed. Thus approximate search results can differ from exact search results.

Depending on data size, the same query can be orders of magnitude faster with the vector index than without it.

## Conclusion

This article demonstrated working with a vector index and an external dataset: creating a table for vectors, loading it from an external dataset, building a vector index on that table, and running vector search with and without the index.

Exact vector search returns the true nearest neighbors but can be expensive, especially on large data. Approximate search returns results much faster with some loss in accuracy, which is often acceptable in practice.

For more on vector indexes, see [here](../../dev/vector-indexes.md).
