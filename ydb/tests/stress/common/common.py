# -*- coding: utf-8 -*-
import ydb
import os
import threading

ydb.interceptor.monkey_patch_event_handler()


class YdbClient:
    def __init__(self, endpoint, database, use_query_service=False):
        self.driver = ydb.Driver(endpoint=endpoint, database=database, oauth=None)
        self.database = database
        self.use_query_service = use_query_service
        self.session_pool = ydb.QuerySessionPool(self.driver) if use_query_service else ydb.SessionPool(self.driver)

    def wait_connection(self, timeout=5):
        self.driver.wait(timeout, fail_fast=True)

    def query(self, statement, is_ddl):
        if self.use_query_service:
            return self.session_pool.execute_with_retries(statement)
        else:
            if is_ddl:
                return self.session_pool.retry_operation_sync(lambda session: session.execute_scheme(statement))
            else:
                raise "Unsuppported dml"  # TODO implement me

    def drop_table(self, path_to_table):
        if self.use_query_service:
            self.session_pool.execute_with_retries(f"DROP TABLE `{path_to_table}`")
        else:
            self.session_pool.retry_operation_sync(lambda session: session.drop_table(path_to_table))

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


class WorkloadBase:
    def __init__(self, client, tables_prefix, workload_name, stop):
        self.client = client
        self.table_prefix = tables_prefix + '/' + workload_name
        self.name = workload_name
        self.stop = stop
        self.workload_threads = []

    def name(self):
        return self.name

    def get_table_path(self, table_name):
        return "/".join([self.client.database, self.table_prefix, table_name])

    def is_stop_requested(self):
        return self.stop.is_set()

    def start(self):
        funcs = self.get_workload_thread_funcs()

        def wrapper(f):
            try:
                f()
            except Exception as e:
                print(f"FATAL: {e}")
                os._exit(1)

        for f in funcs:
            t = threading.Thread(target=lambda: wrapper(f))
            t.start()
            self.workload_threads.append(t)

    def join(self):
        for t in self.workload_threads:
            t.join()
