import logging
import random
import time

from itertools import cycle

from ydb.tests.stress.common.common import WorkloadBase
from ydb.tests.library.fixtures import json

logger = logging.getLogger("JsonIndexWorkload")


class WorkloadJsonIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "json_index", stop)
        self.table_name_prefix = "table"
        self.index_name_prefix = "json_index"
        self.row_count = 200
        self.limit = 20
        self.query_count = 25

    def _create_table(self, table_path, json_document):
        logger.info(f"Create table {table_path}")
        if json_document:
            json_type = "JsonDocument"
        else:
            json_type = "Json"
        create_table_sql = f"""
            CREATE TABLE `{table_path}` (
                pk Uint64,
                json {json_type},
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

    def _create_index(self, index_name, table_path):
        logger.info(f"Create index {index_name}")
        create_index_sql = f"""
            ALTER TABLE `{table_path}`
            ADD INDEX `{index_name}` GLOBAL USING json ON (json);
        """
        self.client.query(create_index_sql, True)

    def _drop_index(self, index_name, table_path):
        logger.info(f"Drop index {index_name}")
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{index_name}`;
        """
        self.client.query(drop_index_sql, True)

    def _upsert_rows(self, table_path, json_document, use_upsert, min_key, max_key):
        logger.info("Upsert rows")
        values = []

        if json_document:
            json_type = "JsonDocument"
        else:
            json_type = "Json"

        for key in range(min_key, max_key):
            json_value = json.get_random_json()
            values.append(f'({key}, {json_type}(\'{json_value}\'))')

        if use_upsert:
            insert = "UPSERT"
        else:
            insert = "INSERT"
        upsert_sql = f"""
            {insert} INTO `{table_path}` (pk, json)
            VALUES {",".join(values)};
        """
        self.client.query(upsert_sql, False)

    def _delete_rows(self, table_path, min_key, max_key):
        logger.info("Delete rows")
        delete_sql = f"""
            DELETE FROM `{table_path}` WHERE pk >= {min_key} AND pk < {max_key};
        """
        self.client.query(delete_sql, False)

    def _select_rows(self, table_path, index_name):
        predicate = json.get_random_predicate()
        select_sql = f"""
            SELECT pk, json
            FROM `{table_path}`
            VIEW `{index_name}`
            WHERE {predicate}
            LIMIT {self.limit};
        """
        res = self.client.query(select_sql, False)
        n = len(res[0].rows)
        logger.info(f"Selected {n} rows using predicate `{predicate}`")
        return n

    def _wait_index_ready(self, index_name, table_path):
        start_time = time.time()
        while time.time() - start_time < 60:
            time.sleep(5)
            try:
                res = self._select_rows(
                    index_name=index_name,
                    table_path=table_path,
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

    def _check_loop(self, table_path, json_document=False):
        if json_document:
            json_type = "JsonDocument"
        else:
            json_type = "Json"
        index_name = f"{self.index_name_prefix}_{json_type}"

        self._create_index(
            index_name=index_name,
            table_path=table_path,
        )

        self._wait_index_ready(
            index_name=index_name,
            table_path=table_path,
        )

        n = 0
        for _ in range(0, self.query_count):
            n += self._select_rows(
                index_name=index_name,
                table_path=table_path,
            )

        if n == 0:
            raise Exception(f"No rows selected with {self.query_count} contains queries")

        self._upsert_rows(
            table_path=table_path,
            json_document=json_document,
            use_upsert=False,
            min_key=self.row_count+1,
            max_key=self.row_count+3,
        )

        self._upsert_rows(
            table_path=table_path,
            json_document=json_document,
            use_upsert=True,
            min_key=self.row_count-3,
            max_key=self.row_count+2,
        )

        self._delete_rows(
            table_path=table_path,
            min_key=self.row_count-3,
            max_key=self.row_count+3,
        )

        if random.randint(0, 1) == 0:
            self._create_index(
                index_name=index_name+'Rename',
                table_path=table_path,
            )
            self.client.replace_index(table_path, index_name+'Rename', index_name)

        self._drop_index(index_name, table_path)
        logger.info(f'Check was completed successfully for table `{table_path}`')

    def _loop(self):
        json_table = self.get_table_path(f"{self.table_name_prefix}_json")
        json_document_table = self.get_table_path(f"{self.table_name_prefix}_json_document")
        tables = [json_table, json_document_table]

        self._create_table(json_table, 0)
        self._create_table(json_document_table, 1)

        json_opts = [0, 1]
        opt_iter = cycle(json_opts)

        while not self.is_stop_requested():
            json_document = next(opt_iter)

            try:
                self._upsert_rows(
                    table_path=tables[json_document],
                    json_document=json_document,
                    use_upsert=True,
                    min_key=0,
                    max_key=self.row_count,
                )

                self._check_loop(
                    table_path=tables[json_document],
                    json_document=json_document,
                )
            except Exception as ex:
                logger.info(f"ERROR: {ex}")
                raise ex
        for t in tables:
            self._drop_table(t)

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
