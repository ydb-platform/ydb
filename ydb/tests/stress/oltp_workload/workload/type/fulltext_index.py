import logging
import random
import time
import re
from itertools import cycle, product

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("FulltextIndexWorkload")

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


class WorkloadFulltextIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "fulltext_index", stop)
        self.table_name_prefix = "table"
        self.index_name_prefix = "fulltext_idx"
        self.row_count = 50
        self.limit = 10
        self.query_count = 10

    def _get_random_text(self):
        t = []
        for i in range(3):
            length = random.randint(3, 17)
            t.extend(self._get_random_phrase(length))
        return ' '.join(t)

    def _get_random_phrase(self, length):
        pos = random.randint(0, len(seed_words)-length+1)
        return seed_words[pos:pos+length]

    def _create_table(self, table_path, utf8):
        logger.info(f"Create table {table_path}")
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        create_table_sql = f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                text {texttype},
                PRIMARY KEY (pk)
            );
        """
        self.client.query(create_table_sql, True)

    def _drop_table(self, table_path):
        logger.info(f"Drop table {table_path}")
        drop_table_sql = f"""
            DROP TABLE `{table_path}`;
        """
        self.client.query(drop_table_sql, True)

    def _drop_index(self, index_name, table_path):
        logger.info(f"Drop index {index_name}")
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{index_name}`;
        """
        self.client.query(drop_index_sql, True)

    def _create_index(
        self, index_name, table_path, layout='flat', tokenizer='standard'
    ):
        logger.info(f"""Creating index layout={layout}, tokenizer={tokenizer}""")
        create_index_sql = f"""
            ALTER TABLE `{table_path}`
            ADD INDEX `{index_name}` GLOBAL USING fulltext
            ON (text)
            WITH (
                layout={layout},
                tokenizer={tokenizer},
                use_filter_lowercase=true,
                use_filter_snowball=true,
                language="english"
            );
        """
        logger.info(create_index_sql)
        self.client.query(create_index_sql, True)

    def _upsert_values(self, table_path, use_upsert, min_key, max_key):
        logger.info("Upsert values")
        values = []

        for key in range(min_key, max_key):
            text = self._get_random_text()
            values.append(f'({key}, "{text}")')

        if use_upsert:
            insert = "UPSERT"
        else:
            insert = "INSERT"
        upsert_sql = f"""
            {insert} INTO `{table_path}` (pk, text)
            VALUES {",".join(values)};
        """
        self.client.query(upsert_sql, False)

    def _delete_rows(self, table_path, min_key, max_key):
        logger.info("Delete rows")
        delete_sql = f"""
            DELETE FROM `{table_path}` WHERE pk >= {min_key} AND pk < {max_key};
        """
        self.client.query(delete_sql, False)

    def _select_contains(self, index_name, table_path, utf8=False):
        if utf8:
            utf8 = "Utf8"
        else:
            utf8 = ""
        query = ' '.join(self._get_random_phrase(3))
        select_sql = f"""
            SELECT `pk`, `text`
            FROM `{table_path}`
            VIEW `{index_name}`
            WHERE FullText::Contains{utf8}(`text`, "{query}")
            LIMIT {self.limit};
        """
        res = self.client.query(select_sql, False)
        if len(res) == 0:
            raise Exception("Query returned no resultsets")
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using contains")
        return n

    def _select_relevance(self, index_name, table_path, utf8=False):
        if utf8:
            utf8 = "Utf8"
        else:
            utf8 = ""
        query = ' '.join(self._get_random_phrase(3))
        select_sql = f"""
            SELECT `pk`, `text`, FullText::Relevance{utf8}(`text`, "{query}") as `rel`
            FROM `{table_path}`
            VIEW `{index_name}`
            ORDER BY `rel`
            LIMIT {self.limit};
        """
        res = self.client.query(select_sql, False)
        if len(res) == 0:
            raise Exception("Query returned no resultsets")
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using relevance")
        prev = -100
        for row in res[0].rows:
            rel = row['rel']
            if rel < prev:
                raise Exception(f"Relevance not in order, prev: {prev}, rel: {rel}")
            prev = rel
        return n

    def _wait_index_ready(self, index_name, table_path, utf8):
        start_time = time.time()
        while time.time() - start_time < 60:
            time.sleep(5)
            try:
                res = self._select_contains(
                    index_name=index_name,
                    table_path=table_path,
                    utf8=utf8,
                )
                if res == 0:
                    continue
            except Exception as ex:
                if "No global indexes for table" in str(ex):
                    continue
                raise ex
            logger.info(f"Index {index_name} is ready")
            return
        raise Exception("Error getting index status")

    def _check_loop(self, table_path, layout='flat', tokenizer='standard', utf8=False):
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        index_name = f"{self.index_name_prefix}_{texttype}_{layout}_{tokenizer}"
        self._create_index(
            table_path=table_path,
            index_name=index_name,
            layout=layout,
            tokenizer=tokenizer,
        )
        self._wait_index_ready(
            table_path=table_path,
            index_name=index_name,
            utf8=utf8,
        )
        n = 0
        for i in range(0, self.query_count):
            # select from index with Fulltext::Contains
            n += self._select_contains(
                index_name=index_name,
                table_path=table_path,
                utf8=utf8,
            )
        if n == 0:
            raise Exception(f"No rows selected with {self.query_count} contains queries")
        if layout == 'flat_relevance':
            n = 0
            for i in range(0, self.query_count):
                # select from index with Fulltext::Relevance
                n += self._select_relevance(
                    index_name=index_name,
                    table_path=table_path,
                    utf8=utf8,
                )
            if n == 0:
                raise Exception(f"No rows selected with {self.query_count} relevance queries")
        # insert into index
        self._upsert_values(
            table_path=table_path,
            use_upsert=False,
            min_key=self.row_count+1,
            max_key=self.row_count+3,
        )
        # update the index using upsert
        self._upsert_values(
            table_path=table_path,
            use_upsert=True,
            min_key=self.row_count-3,
            max_key=self.row_count+2,
        )
        # delete from index
        self._delete_rows(
            table_path=table_path,
            min_key=self.row_count-3,
            max_key=self.row_count,
        )
        # sometimes replace the index
        if random.randint(0, 1) == 0:
            self._create_index(
                index_name=index_name+'Rename',
                table_path=table_path,
                layout=layout,
                tokenizer=tokenizer,
            )
            self.client.replace_index(table_path, index_name+'Rename', index_name)
        self._drop_index(index_name, table_path)
        logger.info('check was completed successfully')

    def _loop(self):
        text_table = self.get_table_path(f"{self.table_name_prefix}_text")
        utf8_table = self.get_table_path(f"{self.table_name_prefix}_utf8")
        tables = [text_table, utf8_table]
        self._create_table(text_table, 0)
        self._create_table(utf8_table, 1)

        utf8_opts = [0, 1]
        layout_opts = ['flat', 'flat_relevance']
        tokenizer_opts = ['standard', 'whitespace']
        opts = list(product(utf8_opts, layout_opts, tokenizer_opts))
        random.shuffle(opts)
        opt_iter = cycle(opts)

        while not self.is_stop_requested():
            [utf8, layout, tokenizer] = next(opt_iter)
            try:
                self._upsert_values(
                    table_path=tables[utf8],
                    use_upsert=True,
                    min_key=0,
                    max_key=self.row_count,
                )
                self._check_loop(
                    table_path=tables[utf8],
                    layout=layout,
                    tokenizer=tokenizer,
                    utf8=utf8,
                )
            except Exception as ex:
                logger.info(f"ERROR {ex}")
                raise ex
        for t in tables:
            self._drop_table(t)

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
