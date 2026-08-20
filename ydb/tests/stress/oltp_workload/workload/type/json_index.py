import logging
import os
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
        self.model_seed = int(os.getenv("YDB_JSON_INDEX_SEED", "19088743"), 0)
        self.model_marker = f"json-index-model-{self.model_seed}"
        self.iteration = 0
        logger.info(
            "JSON index model seed=%d (override with YDB_JSON_INDEX_SEED)",
            self.model_seed,
        )

    def _create_table(self, table_path, json_document, string_pk=False):
        logger.info(f"Create table {table_path}")
        if json_document:
            json_type = "JsonDocument"
        else:
            json_type = "Json"
        pk_type = "Utf8 NOT NULL" if string_pk else "Uint64"
        create_table_sql = f"""
            CREATE TABLE `{table_path}` (
                pk {pk_type},
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

    def _model_keys(self, string_pk):
        if string_pk:
            return [f"model-{self.model_seed:x}-{slot}" for slot in range(4)]
        base = 1_000_000 + (self.model_seed % 10_000) * 10
        return [base + slot for slot in range(4)]

    @staticmethod
    def _key_literal(key, string_pk):
        return f'"{key}"u' if string_pk else str(key)

    @staticmethod
    def _json_type(json_document):
        return "JsonDocument" if json_document else "Json"

    def _model_json(self, json_document, bucket, generation):
        json_type = self._json_type(json_document)
        return (
            f"{json_type}('{{\"stress_marker\":\"{self.model_marker}\","
            f"\"bucket\":\"{bucket}\",\"generation\":{generation}}}')"
        )

    def _write_model_rows(self, table_path, json_document, string_pk, verb, rows):
        values = ",".join(
            f"({self._key_literal(key, string_pk)}, "
            f"{self._model_json(json_document, bucket, generation)})"
            for key, bucket, generation in rows
        )
        self.client.query(f"""
            {verb} INTO `{table_path}` (pk, json) VALUES {values};
        """, False)

    def _delete_model_rows(self, table_path, string_pk, keys):
        key_list = ",".join(self._key_literal(key, string_pk) for key in keys)
        self.client.query(f"""
            DELETE FROM `{table_path}` WHERE pk IN ({key_list});
        """, False)

    def _model_predicate(self):
        return (
            f'JSON_VALUE(json, \'$.stress_marker\' RETURNING Utf8) = "{self.model_marker}"u '
            'AND JSON_VALUE(json, \'$.bucket\' RETURNING Utf8) = "match"u'
        )

    def _select_model_keys(self, table_path, index_name, view):
        result = self.client.query(f"""
            SELECT pk FROM `{table_path}` {view}
            WHERE {self._model_predicate()}
            ORDER BY pk;
        """, False)
        return [row['pk'] for row in result[0].rows]

    def _assert_model_oracle(self, table_path, index_name, expected):
        primary = self._select_model_keys(table_path, index_name, "VIEW PRIMARY KEY")
        explicit = self._select_model_keys(table_path, index_name, f"VIEW `{index_name}`")
        automatic = self._select_model_keys(table_path, index_name, "")
        expected = sorted(expected)
        if primary != expected or explicit != expected or automatic != expected:
            raise AssertionError(
                f"JSON model mismatch seed={self.model_seed} table={table_path}: "
                f"expected={expected}, primary={primary}, explicit={explicit}, auto={automatic}"
            )

    def _wait_model_index_ready(self, table_path, index_name):
        start_time = time.time()
        while time.time() - start_time < 60:
            try:
                self._select_model_keys(table_path, index_name, f"VIEW `{index_name}`")
                return
            except Exception as error:
                message = str(error)
                if ("No global indexes for table" in message or
                        "not ready to use" in message or
                        "Required global index not found" in message):
                    time.sleep(1)
                    continue
                raise
        raise Exception(f"Model index {index_name} did not become ready, seed={self.model_seed}")

    def _mutate_and_check_model(self, table_path, index_name, json_document, string_pk, state):
        keys = self._model_keys(string_pk)
        self._write_model_rows(
            table_path, json_document, string_pk, "UPSERT",
            [(keys[1], "match", 2), (keys[2], "other", 2)],
        )
        state[keys[1]] = "match"
        state[keys[2]] = "other"
        self._assert_model_oracle(
            table_path, index_name, [key for key, bucket in state.items() if bucket == "match"])

        self._delete_model_rows(table_path, string_pk, [keys[0]])
        del state[keys[0]]
        self._assert_model_oracle(
            table_path, index_name, [key for key, bucket in state.items() if bucket == "match"])
        return state

    def _insert_and_check_model(self, table_path, index_name, json_document, string_pk):
        keys = self._model_keys(string_pk)
        state = dict(zip(keys, ("match", "other", "match", "other")))
        self._write_model_rows(
            table_path, json_document, string_pk, "INSERT",
            [(key, bucket, 1) for key, bucket in state.items()],
        )
        self._assert_model_oracle(
            table_path, index_name, [key for key, bucket in state.items() if bucket == "match"])
        return self._mutate_and_check_model(
            table_path, index_name, json_document, string_pk, state)

    def _check_string_pk_row_id(self, table_path):
        index_name = f"{self.index_name_prefix}_StringPk"
        keys = self._model_keys(True)
        state = dict(zip(keys, ("match", "other", "match", "other")))

        # Seed the table before build so row-id allocation and snapshot ingestion
        # are both exercised for a non-integer primary key.
        self._write_model_rows(
            table_path, True, True, "INSERT",
            [(key, bucket, 1) for key, bucket in state.items()],
        )
        self._create_index(index_name, table_path)
        self._wait_model_index_ready(table_path, index_name)
        self._assert_model_oracle(
            table_path, index_name, [key for key, bucket in state.items() if bucket == "match"])
        state = self._mutate_and_check_model(table_path, index_name, True, True, state)

        replacement = index_name + "Rename"
        self._create_index(replacement, table_path)
        self._wait_model_index_ready(table_path, replacement)
        self.client.replace_index(table_path, replacement, index_name)
        self._assert_model_oracle(
            table_path, index_name, [key for key, bucket in state.items() if bucket == "match"])

        self._delete_model_rows(table_path, True, list(state))
        self._assert_model_oracle(table_path, index_name, [])
        self._drop_index(index_name, table_path)

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

        model_state = self._insert_and_check_model(
            table_path, index_name, json_document, False)

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

        # Keep the old roughly-every-other-iteration replacement load, but make
        # its schedule reproducible and always verify it against the model.
        if self.iteration % 2 == 0:
            self._create_index(
                index_name=index_name+'Rename',
                table_path=table_path,
            )
            self._wait_model_index_ready(table_path, index_name+'Rename')
            self.client.replace_index(table_path, index_name+'Rename', index_name)
            self._assert_model_oracle(
                table_path, index_name,
                [key for key, bucket in model_state.items() if bucket == "match"],
            )

        self._delete_model_rows(table_path, False, list(model_state))
        self._assert_model_oracle(table_path, index_name, [])

        self._drop_index(index_name, table_path)
        logger.info(f'Check was completed successfully for table `{table_path}`')

    def _loop(self):
        json_table = self.get_table_path(f"{self.table_name_prefix}_json")
        json_document_table = self.get_table_path(f"{self.table_name_prefix}_json_document")
        string_pk_table = self.get_table_path(f"{self.table_name_prefix}_json_document_string_pk")
        tables = [json_table, json_document_table]

        self._create_table(json_table, 0)
        self._create_table(json_document_table, 1)
        self._create_table(string_pk_table, 1, string_pk=True)

        json_opts = [0, 1]
        opt_iter = cycle(json_opts)

        while not self.is_stop_requested():
            self.iteration += 1
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
                if json_document:
                    self._check_string_pk_row_id(string_pk_table)
            except Exception as ex:
                logger.info(f"ERROR: {ex}")
                raise ex
        for t in tables + [string_pk_table]:
            self._drop_table(t)

    def get_stat(self):
        return ""

    def get_workload_thread_funcs(self):
        return [self._loop]
