# -*- coding: utf-8 -*-

import random
import re

seed_text = """The `vector_kmeans_tree` index implements hierarchical data clustering. The structure of the index includes:

1. Hierarchical clustering:

    * the index builds multiple levels of k-means clusters
    * at each level, vectors are distributed across a predefined number of clusters raised to the power of the level
    * the first level clusters the entire dataset
    * subsequent levels recursively cluster the contents of each parent cluster

2. Search process:

    * search proceeds recursively from the first level to the subsequent ones
    * during queries, the index analyzes only the most promising clusters
    * such search space pruning avoids complete enumeration of all vectors

3. Parameters:

    * `levels`: number of levels in the tree, defining search depth (recommended 1-3)
    * `clusters`: number of clusters in k-means, defining search width (recommended 64-512)

Internally, a vector index consists of hidden index tables named `indexImpl*Table`. In selection queries using the vector index,
the index tables will appear in query statistics.

Data in a YDB table is always sorted by the primary key. That means that retrieving any entry from the table with specified field
values comprising the primary key always takes the minimum fixed time, regardless of the total number of table entries. Indexing
by the primary key makes it possible to retrieve any consecutive range of entries in ascending or descending order of the primary key.
Execution time for this operation depends only on the number of retrieved entries rather than on the total number of table records.

To use a similar feature with any field or combination of fields, additional indexes called **secondary indexes** can be created for them.

In transactional systems, indexes are used to limit or avoid performance degradation and increase of query cost as your data grows.

This article describes the main operations with secondary indexes and gives references to detailed information on each operation.
For more information about various types of secondary indexes and their specifics, see Secondary indexes in the Concepts section.

If you need to push data to multiple tables, we recommend pushing data to a single table within a single query.

If you need to push data to a table with a synchronous secondary index, we recommend that you first push data to a table and, when
done, build a secondary index.

You should avoid writing data sequentially in ascending or descending order of the primary key.  Writing data to a table with a
monotonically increasing key causes all new data to be written to the end of the table since all tables in {{ ydb-short-name }}
are sorted by ascending primary key. As {{ ydb-short-name }} splits table data into shards based on key ranges, inserts are always
processed by the same server responsible for the last shard. Concentrating the load on a single server will result in slow data
uploading and inefficient use of a distributed system.

Some use cases require writing the initial data (often large amounts) to a table before enabling OLTP workloads. In this case,
transactionality at the level of individual queries is not required, and you can use `BulkUpsert` calls in the
[CLI](../reference/ydb-cli/export-import/tools-restore.md), [SDK](../recipes/ydb-sdk/bulk-upsert.md) and API. Since no transactionality
is used, this approach has a much lower overhead than YQL queries. In case of a successful response to the query, the `BulkUpsert`
method guarantees that all data added within this query is committed."""

seed_words = re.split(r'\s+', seed_text)


def get_random_words(length):
    pos = random.randint(0, len(seed_words)-length+1)
    return seed_words[pos:pos+length]


def get_random_text(iters=3, minlen=3, maxlen=17):
    t = []
    for i in range(iters):
        length = random.randint(minlen, maxlen)
        t.extend(get_random_words(length))
    return ' '.join(t)
