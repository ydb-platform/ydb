# -*- coding: utf-8 -*-
import ydb
import os
import threading
import multiprocessing
import logging
from typing import Optional

ydb.interceptor.monkey_patch_event_handler()

logger = logging.getLogger(__name__)


class YdbClient:
    def __init__(self, endpoint, database, use_query_service=False, sessions=100):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.use_query_service = use_query_service
        self.session_pool = ydb.QuerySessionPool(self.driver, size=sessions) if use_query_service else ydb.SessionPool(self.driver, size=sessions)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement, is_ddl, parameters=None, retry_settings=None, log_error=True):
        if self.use_query_service:
            try:
                return self.session_pool.execute_with_retries(query=statement, parameters=parameters, retry_settings=retry_settings)
            except Exception as e:
                if log_error:
                    logger.error(f"Error: {e} while executing query: {statement}")
                raise e
        else:
            if is_ddl:
                return self.session_pool.retry_operation_sync(lambda session: session.execute_scheme(statement))
            else:
                return self.session_pool.retry_operation_sync(lambda session: session.transaction().execute(statement, parameters=parameters, commit_tx=True))

    def drop_table(self, path_to_table):
        if self.use_query_service:
            self.query(f"DROP TABLE `{path_to_table}`", True)
        else:
            self.session_pool.retry_operation_sync(lambda session: session.drop_table(path_to_table))

    def replace_index(self, table, src, dst):
        self.driver.table_client.alter_table(path=table, rename_indexes=[
            ydb.RenameIndexItem(source_name=src, destination_name=dst, replace_destination=True)
        ])

    def describe(self, path):
        try:
            return self.driver.scheme_client.describe_path(path)
        except ydb.issues.SchemeError as e:
            if "Path not found" in e.message:
                return None
            raise e

    def _remove_recursively(self, path):
        deleted = 0
        d = self.driver.scheme_client.list_directory(path)
        for entry in d.children:
            entry_path = "/".join([path, entry.name])
            if entry.is_directory():
                deleted += self._remove_recursively(entry_path)
            elif entry.is_column_table() or entry.is_table():
                self.drop_table(entry_path)
                deleted += 1
            else:
                raise f"Scheme entry {entry_path} of unexpected type"
        self.driver.scheme_client.remove_directory(path)
        return deleted

    def remove_recursively(self, path):
        d = self.describe(path)
        if d is None:
            return
        if not d.is_directory():
            raise f"{path} has unexpected type"
        return self._remove_recursively(path)

    def close(self):
        self.session_pool.stop()
        self.driver.stop()


class WorkloadBase:
    def __init__(self, client, tables_prefix, workload_name, stop):
        self.client = client
        self.table_prefix = tables_prefix + '/' + workload_name
        self.name = workload_name
        self.stop = stop
        self.workload_entities = []
        self.use_multiprocessing = False

    def name(self):
        return self.name

    def get_table_path(self, table_name):
        return "/".join([self.client.database, self.table_prefix, table_name])

    def is_stop_requested(self):
        return self.stop.is_set()

    def start(self, use_multiprocessing: bool = False):
        self.use_multiprocessing = use_multiprocessing

        if hasattr(self, '_pre_start'):
            if not self._pre_start():
                return False

        funcs = self.get_workload_thread_funcs()

        def wrapper(f):
            try:
                f()
            except Exception as e:
                logger.exception(f"FATAL: {e}")
                os._exit(1)

        entity_factory = multiprocessing.Process if self.use_multiprocessing else threading.Thread
        for f in funcs:
            p = entity_factory(target=lambda: wrapper(f))
            p.start()
            self.workload_entities.append(p)

        return True

    def join(self, timeout: Optional[float] = None):
        for t in self.workload_entities:
            t.join(timeout)

    def wait_stop(self, timeout: Optional[float] = None) -> bool:
        self.join(timeout)
        if hasattr(self, '_post_stop'):
            if not self._post_stop():
                return False
        return True

    def is_alive(self) -> bool:
        return any(t.is_alive() for t in self.workload_entities)

    def terminate(self):
        if self.use_multiprocessing:
            for p in self.workload_entities:
                if p.is_alive():
                    p.terminate()
