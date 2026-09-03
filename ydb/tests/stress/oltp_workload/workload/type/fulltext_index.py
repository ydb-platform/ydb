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
        # Number of distinct prefix (user_id) values in the prefixed tables.
        # Every prefixed query matches only one of these groups, so the number of
        # groups is kept small to leave enough rows in each of them - otherwise
        # random word queries return nothing too often.
        self.user_count = 2

    def _pk_type(self, string_pk):
        return "String" if string_pk else "Uint64"

    def _pk_literal(self, key, string_pk):
        # Zero-padded strings keep lexical order equal to numeric order for range deletes.
        if string_pk:
            return f'"{key:010d}"'
        return str(key)

    def _create_table(self, table_path, utf8, with_prefix=False, string_pk=False):
        logger.info(f"Create table {table_path}")
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        pktype = self._pk_type(string_pk)
        if with_prefix:
            create_table_sql = f"""
                CREATE TABLE `{table_path}` (
                    pk {pktype},
                    user_id Uint64,
                    text {texttype},
                    PRIMARY KEY (pk)
                );
            """
        else:
            create_table_sql = f"""
                CREATE TABLE `{table_path}` (
                    pk {pktype},
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
        self, index_name, table_path, index_type, tokenizer='standard', with_prefix=False
    ):
        logger.info(f"""Creating index index_type={index_type}, tokenizer={tokenizer}, with_prefix={with_prefix}""")
        if with_prefix:
            create_index_sql = f"""
                ALTER TABLE `{table_path}`
                ADD INDEX `{index_name}` GLOBAL USING {index_type}
                ON (user_id, text)
                WITH (
                    tokenizer={tokenizer},
                    use_filter_lowercase=true,
                    use_filter_snowball=true,
                    language="english"
                );
            """
        else:
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

    def _upsert_values(self, table_path, use_upsert, min_key, max_key, with_prefix=False, string_pk=False):
        logger.info("Upsert values")
        values = []

        for key in range(min_key, max_key):
            text = fulltext.get_random_text()
            pk = self._pk_literal(key, string_pk)
            if with_prefix:
                user_id = (key % self.user_count) + 1
                values.append(f'({pk}, {user_id}, "{text}")')
            else:
                values.append(f'({pk}, "{text}")')

        if use_upsert:
            insert = "UPSERT"
        else:
            insert = "INSERT"
        if with_prefix:
            upsert_sql = f"""
                {insert} INTO `{table_path}` (pk, user_id, text)
                VALUES {",".join(values)};
            """
        else:
            upsert_sql = f"""
                {insert} INTO `{table_path}` (pk, text)
                VALUES {",".join(values)};
            """
        self.client.query(upsert_sql, False)

    def _delete_rows(self, table_path, min_key, max_key, string_pk=False):
        logger.info("Delete rows")
        min_pk = self._pk_literal(min_key, string_pk)
        max_pk = self._pk_literal(max_key, string_pk)
        delete_sql = f"""
            DELETE FROM `{table_path}` WHERE pk >= {min_pk} AND pk < {max_pk};
        """
        self.client.query(delete_sql, False)

    def _select_contains(self, index_name, table_path, with_prefix=False):
        query = ' '.join(fulltext.get_random_words(3))
        if with_prefix:
            user_id = random.randint(1, self.user_count)
            select_sql = f"""
                SELECT `pk`, `text`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE user_id = {user_id} AND FulltextMatch(`text`, "{query}")
                LIMIT {self.limit};
            """
        else:
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

    def _select_relevance(self, index_name, table_path, with_prefix=False):
        query = ' '.join(fulltext.get_random_words(3))
        if with_prefix:
            user_id = random.randint(1, self.user_count)
            select_sql = f"""
                SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE user_id = {user_id} AND FulltextScore(`text`, "{query}") > 0
                ORDER BY `rel`
                LIMIT {self.limit};
            """
        else:
            select_sql = f"""
                SELECT `pk`, `text`, FulltextScore(`text`, "{query}") as `rel`
                FROM `{table_path}`
                VIEW `{index_name}`
                WHERE FulltextScore(`text`, "{query}") > 0
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

    def _wait_index_ready(self, index_name, table_path, with_prefix=False):
        start_time = time.time()
        while time.time() - start_time < 60:
            time.sleep(5)
            try:
                res = self._select_contains(
                    index_name=index_name,
                    table_path=table_path,
                    with_prefix=with_prefix,
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

    def _check_loop(
        self, table_path, index_type, tokenizer='standard', utf8=False, with_prefix=False, string_pk=False
    ):
        if utf8:
            texttype = "Utf8"
        else:
            texttype = "String"
        prefix_suffix = "_prefixed" if with_prefix else ""
        pk_suffix = "_string_pk" if string_pk else ""
        index_name = f"{self.index_name_prefix}_{texttype}_{index_type}_{tokenizer}{prefix_suffix}{pk_suffix}"
        self._create_index(
            table_path=table_path,
            index_name=index_name,
            index_type=index_type,
            tokenizer=tokenizer,
            with_prefix=with_prefix,
        )
        self._wait_index_ready(
            table_path=table_path,
            index_name=index_name,
            with_prefix=with_prefix,
        )
        n = 0
        for i in range(0, self.query_count):
            # select from index with FulltextMatch
            n += self._select_contains(
                index_name=index_name,
                table_path=table_path,
                with_prefix=with_prefix,
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
                    with_prefix=with_prefix,
                )
            if n == 0:
                raise Exception(f"No rows selected with {self.query_count} relevance queries")
        # insert into index
        self._upsert_values(
            table_path=table_path,
            use_upsert=False,
            min_key=self.row_count+1,
            max_key=self.row_count+3,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        # update the index using upsert
        self._upsert_values(
            table_path=table_path,
            use_upsert=True,
            min_key=self.row_count-3,
            max_key=self.row_count+2,
            with_prefix=with_prefix,
            string_pk=string_pk,
        )
        # delete from index
        self._delete_rows(
            table_path=table_path,
            min_key=self.row_count-3,
            max_key=self.row_count+3,
            string_pk=string_pk,
        )
        # sometimes replace the index
        if random.randint(0, 1) == 0:
            self._create_index(
                index_name=index_name+'Rename',
                table_path=table_path,
                index_type=index_type,
                tokenizer=tokenizer,
                with_prefix=with_prefix,
            )
            self.client.replace_index(table_path, index_name+'Rename', index_name)
        self._drop_index(index_name, table_path)
        logger.info('check was completed successfully')

    def _loop(self):
        # Tables cover text type × prefix × PK type (Uint64 legacy doc_id vs String -> __ydb_row_id).
        table_specs = [
            (False, False, False),  # String text, no prefix, Uint64 PK
            (True, False, False),   # Utf8 text, no prefix, Uint64 PK
            (False, True, False),   # String text, prefixed, Uint64 PK
            (True, True, False),    # Utf8 text, prefixed, Uint64 PK
            (False, False, True),   # String text, no prefix, String PK
            (True, False, True),    # Utf8 text, no prefix, String PK
            (False, True, True),    # String text, prefixed, String PK
            (True, True, True),     # Utf8 text, prefixed, String PK
        ]
        tables = []
        for utf8, with_prefix, string_pk in table_specs:
            text_suffix = "utf8" if utf8 else "text"
            prefix_suffix = "_prefixed" if with_prefix else ""
            pk_suffix = "_string_pk" if string_pk else ""
            table_path = self.get_table_path(
                f"{self.table_name_prefix}_{text_suffix}{prefix_suffix}{pk_suffix}"
            )
            self._create_table(
                table_path, utf8, with_prefix=with_prefix, string_pk=string_pk
            )
            tables.append(table_path)

        utf8_opts = [0, 1]
        index_type_opts = ['fulltext_plain', 'fulltext_relevance']
        tokenizer_opts = ['standard', 'whitespace']
        prefix_opts = [False, True]
        string_pk_opts = [False, True]
        opts = list(product(utf8_opts, index_type_opts, tokenizer_opts, prefix_opts, string_pk_opts))
        random.shuffle(opts)
        opt_iter = cycle(opts)

        while not self.is_stop_requested():
            [utf8, index_type, tokenizer, with_prefix, string_pk] = next(opt_iter)
            try:
                # Same layout as table_specs: utf8 + 2*prefix + 4*string_pk
                table_idx = utf8 + (2 if with_prefix else 0) + (4 if string_pk else 0)
                self._upsert_values(
                    table_path=tables[table_idx],
                    use_upsert=True,
                    min_key=0,
                    max_key=self.row_count,
                    with_prefix=with_prefix,
                    string_pk=string_pk,
                )
                self._check_loop(
                    table_path=tables[table_idx],
                    index_type=index_type,
                    tokenizer=tokenizer,
                    utf8=utf8,
                    with_prefix=with_prefix,
                    string_pk=string_pk,
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
