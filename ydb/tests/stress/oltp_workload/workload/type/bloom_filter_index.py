import logging
import random

from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger("BloomFilterIndexWorkload")


class WorkloadBloomFilterIndex(WorkloadBase):
    def __init__(self, client, prefix, stop):
        super().__init__(client, prefix, "bloom_filter_index", stop)
        self.table_name_prefix = "bloom_table"
        self.row_count = 100

    def _create_table(self, table_path, num_key_columns):
        logger.info(f"Create table {table_path} with {num_key_columns} key columns")
        key_cols = ", ".join([f"k{i} Uint64" for i in range(num_key_columns)])
        pk_cols = ", ".join([f"k{i}" for i in range(num_key_columns)])
        sql = f"""
            CREATE TABLE `{table_path}` (
                {key_cols},
                value Utf8,
                PRIMARY KEY ({pk_cols})
            );
        """
        self.client.query(sql, True)

    def _drop_table(self, table_path):
        logger.info(f"Drop table {table_path}")
        self.client.query(f"DROP TABLE `{table_path}`;", True)

    def _add_bloom_index(self, table_path, index_name, prefix_columns, fpp=None):
        cols = ", ".join(prefix_columns)
        with_clause = ""
        if fpp is not None:
            with_clause = f" WITH (false_positive_probability={fpp})"
        sql = f"""
            ALTER TABLE `{table_path}`
            ADD INDEX `{index_name}` LOCAL USING bloom_filter ON ({cols}){with_clause};
        """
        logger.info(f"Add bloom index: {sql.strip()}")
        self.client.query(sql, True)

    def _disable_bloom(self, table_path):
        logger.info(f"Disable bloom filter on {table_path}")
        self.client.query(f"ALTER TABLE `{table_path}` SET (KEY_BLOOM_FILTER = DISABLED);", True)

    def _upsert_rows(self, table_path, num_key_columns, min_key, max_key):
        col_names = ", ".join([f"k{i}" for i in range(num_key_columns)] + ["value"])
        rows = []
        for key in range(min_key, max_key):
            key_vals = ", ".join([str(key + i * 1000) for i in range(num_key_columns)])
            rows.append(f'({key_vals}, "val_{key}"u)')
        sql = f"UPSERT INTO `{table_path}` ({col_names}) VALUES {', '.join(rows)};"
        self.client.query(sql, False)

    def _select_existing(self, table_path, num_key_columns):
        key = random.randint(0, self.row_count - 1)
        prefix_len = random.randint(1, num_key_columns)
        where_clauses = [f"k{i} = {key + i * 1000}" for i in range(prefix_len)]
        sql = f"SELECT * FROM `{table_path}` WHERE {' AND '.join(where_clauses)};"
        return self.client.query(sql, False)

    def _select_nonexistent(self, table_path, num_key_columns):
        key = self.row_count + random.randint(1000, 9999)
        prefix_len = random.randint(1, num_key_columns)
        where_clauses = [f"k{i} = {key + i * 1000}" for i in range(prefix_len)]
        sql = f"SELECT * FROM `{table_path}` WHERE {' AND '.join(where_clauses)};"
        return self.client.query(sql, False)

    def _check_loop(self, table_path, num_key_columns):
        key_col_names = [f"k{i}" for i in range(num_key_columns)]

        # Add bloom indexes with various prefix lengths and FPP values
        fpp_values = [None, 0.01, 0.1]
        indexes = []
        for prefix_len in range(1, num_key_columns + 1):
            fpp = random.choice(fpp_values)
            index_name = f"bloom_idx_{prefix_len}"
            self._add_bloom_index(table_path, index_name, key_col_names[:prefix_len], fpp)
            indexes.append(index_name)

        # Insert data
        self._upsert_rows(table_path, num_key_columns, 0, self.row_count)

        # Lookups by various prefixes; existing and non-existing prefixes exercise bloom filter behavior.
        for _ in range(10):
            res_ext = self._select_existing(table_path, num_key_columns)
            assert len(res_ext[0].rows) == 1, f"Expected 1 row, got {len(res_ext[0].rows)}"
            res_non = self._select_nonexistent(table_path, num_key_columns)
            assert len(res_non[0].rows) == 0, f"Expected 0 rows, got {len(res_non[0].rows)}"

        # Disable all bloom filters
        self._disable_bloom(table_path)

        logger.info("check was completed successfully")

    def _loop(self):
        iteration = 0
        while not self.is_stop_requested():
            num_key_columns = random.randint(1, 3)
            table_path = self.get_table_path(f"{self.table_name_prefix}_{iteration}")
            try:
                self._create_table(table_path, num_key_columns)
                self._check_loop(table_path, num_key_columns)
                self._drop_table(table_path)
            except Exception as ex:
                logger.info(f"ERROR {ex}")
                raise ex
            iteration += 1

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
