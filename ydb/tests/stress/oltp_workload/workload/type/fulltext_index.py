import logging
import random
import time
from itertools import cycle, product

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.fixtures import fulltext

logger = logging.getLogger("FulltextIndexWorkload")


class WorkloadFulltextIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "fulltext_index", stop)
        self.table_name_prefix = "table"
        self.index_name_prefix = "fulltext_idx"
        self.row_count = 50
        self.limit = 10
        self.query_count = 10

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
        self, index_name, table_path, index_type, tokenizer='standard'
    ):
        logger.info(f"""Creating index index_type={index_type}, tokenizer={tokenizer}""")
        create_index_sql = f"""
            ALTER TABLE `{table_path}`
            ADD INDEX `{index_name}` GLOBAL USING {index_type}
            ON (text)
            WITH (
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
            text = fulltext.get_random_text()
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

    def _select_contains(self, index_name, table_path):
        query = ' '.join(fulltext.get_random_words(3))
        select_sql = f"""
            SELECT `pk`, `text`
            FROM `{table_path}`
            VIEW `{index_name}`
            WHERE FulltextMatch(`text`, "{query}")
            LIMIT {self.limit};
        """
        res = self.client.query(select_sql, False)
        if len(res) == 0:
            raise Exception("Query returned no resultsets")
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using contains")
        return n

    def _select_relevance(self, index_name, table_path):
        query = ' '.join(fulltext.get_random_words(3))
        select_sql = f"""
            SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
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

    def _check_loop(self, table_path, index_type, tokenizer='standard', utf8=False):
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        index_name = f"{self.index_name_prefix}_{texttype}_{index_type}_{tokenizer}"
        self._create_index(
            table_path=table_path,
            index_name=index_name,
            index_type=index_type,
            tokenizer=tokenizer,
        )
        self._wait_index_ready(
            table_path=table_path,
            index_name=index_name,
            utf8=utf8,
        )
        n = 0
        for i in range(0, self.query_count):
            # select from index with FulltextMatch
            n += self._select_contains(
                index_name=index_name,
                table_path=table_path,
            )
        if n == 0:
            raise Exception(f"No rows selected with {self.query_count} contains queries")
        if index_type == 'fulltext_relevance':
            n = 0
            for i in range(0, self.query_count):
                # select from index with FulltextScore
                n += self._select_relevance(
                    index_name=index_name,
                    table_path=table_path,
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
                index_type=index_type,
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
        index_type_opts = ['fulltext_plain', 'fulltext_relevance']
        tokenizer_opts = ['standard', 'whitespace']
        opts = list(product(utf8_opts, index_type_opts, tokenizer_opts))
        random.shuffle(opts)
        opt_iter = cycle(opts)

        while not self.is_stop_requested():
            [utf8, index_type, tokenizer] = next(opt_iter)
            try:
                self._upsert_values(
                    table_path=tables[utf8],
                    use_upsert=True,
                    min_key=0,
                    max_key=self.row_count,
                )
                self._check_loop(
                    table_path=tables[utf8],
                    index_type=index_type,
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
