# -*- coding: utf-8 -*-
import ydb
import logging
import time
import os
import random
import string
import threading

logger = logging.getLogger("ShowCreateViewWorkload")


def get_unique_suffix(k=5):
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=k))


class ShowCreateViewWorkload:
    def __init__(self, endpoint, database, duration, path_prefix=None):
        self.endpoint = endpoint
        self.database = database
        self.duration = duration

        self.driver_config = ydb.DriverConfig(
            self.endpoint,
            self.database,
        )
        self.driver = ydb.Driver(self.driver_config)
        self.driver.wait(timeout=10, fail_fast=True)
        logger.info(f"Driver initialized successfully for endpoint {self.endpoint}, database {self.database}")

        self.pool = ydb.QuerySessionPool(self.driver)

        instance_id = get_unique_suffix()

        base_table_name = f"t_{instance_id}"
        base_view_name = f"v_{instance_id}"

        if path_prefix:
            self.table_path = os.path.join(path_prefix.strip("/"), base_table_name)
            self.view_path = os.path.join(path_prefix.strip("/"), base_view_name)
        else:
            self.table_path = base_table_name
            self.view_path = base_view_name

        logger.info(f"Target table relative path: {self.table_path}")
        logger.info(f"Target view relative path: {self.view_path}")

        self.successful_cycles = 0
        self.failed_cycles = 0
        self.no_signal_cycles = 0

        self._stats_lock = threading.Lock()
        self._stop_event = threading.Event()
        self.overall_workload_error_occurred = False

    def _increment_successful_cycles(self):
        with self._stats_lock:
            self.successful_cycles += 1

    def _increment_failed_cycles(self):
        with self._stats_lock:
            self.failed_cycles += 1
            self.overall_workload_error_occurred = True

    def _increment_no_signal_cycles(self):
        with self._stats_lock:
            self.no_signal_cycles += 1

    def _mark_overall_workload_error(self):
        with self._stats_lock:
            self.overall_workload_error_occurred = True

    def preliminary_setup(self):
        logger.info(f"Performing preliminary setup for table `{self.table_path}`...")

        create_table_query = f"""
        CREATE TABLE `{self.table_path}` (
            key Int32,
            value Utf8,
            PRIMARY KEY (key)
        );
        """
        self.pool.execute_with_retries(create_table_query)
        logger.info(f"Table `{self.table_path}` created.")

        upsert_query = f"""
        UPSERT INTO `{self.table_path}` (key, value) VALUES
            (1, "one"), (2, "two"), (3, "three");
        """
        self.pool.execute_with_retries(upsert_query)
        logger.info(f"Data upserted into `{self.table_path}`.")

        logger.info("Preliminary setup completed.")

    def cleanup_resources(self):
        logger.info(f"Cleaning up view `{self.view_path}` and table `{self.table_path}`...")

        try:
            drop_view_query = f"DROP VIEW IF EXISTS `{self.view_path}`;"
            self.pool.execute_with_retries(drop_view_query)
            logger.info(f"View `{self.view_path}` dropped.")
        except Exception as e:
            logger.warning(f"Failed to drop view `{self.view_path}` during cleanup: {e}")

        try:
            drop_table_query = f"DROP TABLE IF EXISTS `{self.table_path}`;"
            self.pool.execute_with_retries(drop_table_query)
            logger.info(f"Table `{self.table_path}` dropped.")
        except Exception as e:
            logger.warning(f"Failed to drop table `{self.table_path}` during cleanup: {e}")

        logger.info("Cleanup finished.")

    def _recreation_loop(self):
        thread_name = threading.current_thread().name
        logger.info(f"[{thread_name}] started.")
        while not self._stop_event.is_set():
            # Drop View
            drop_view_query = f"DROP VIEW IF EXISTS `{self.view_path}`;"
            self.pool.execute_with_retries(drop_view_query)
            logger.debug(f"[{thread_name}] View `{self.view_path}` dropped.")

            # Create View
            create_view_query = f"""
            CREATE VIEW `{self.view_path}` WITH security_invoker = TRUE AS
                SELECT * FROM `{self.table_path}`;
            """
            self.pool.execute_with_retries(create_view_query)
            logger.debug(f"[{thread_name}] View `{self.view_path}` created.")

            # Let the created view exist for at least some time.
            time.sleep(0.2)

        logger.info(f"[{thread_name}] Worker stopped.")

    def _showing_loop(self):
        thread_name = threading.current_thread().name
        logger.info(f"[{thread_name}] started.")

        while not self._stop_event.is_set():
            select_was_successful = False
            select_had_critical_error = False
            show_was_successful = False
            show_had_critical_error = False

            # Step 1: SELECT * FROM v
            try:
                select_query = f"SELECT * FROM `{self.view_path}`;"
                result_sets = self.pool.execute_with_retries(select_query)
                row_count = len(result_sets[0].rows) if result_sets and result_sets[0].rows else 0

                if row_count == 3:
                    select_was_successful = True
                else:
                    logger.error(
                        f"[{thread_name}] SELECT * FROM `{self.view_path}` returned {row_count} rows, expected 3."
                    )
                    select_had_critical_error = True
            except ydb.SchemeError:
                logger.debug(f"[{thread_name}] SELECT * FROM `{self.view_path}` failed as expected (view likely gone).")
            except Exception as e:
                logger.warning(f"[{thread_name}] SELECT * FROM `{self.view_path}` failed with other error: {e}")
                select_had_critical_error = True

            # Step 2: SHOW CREATE VIEW v
            try:
                show_create_query = f"SHOW CREATE VIEW `{self.view_path}`;"
                result_sets = self.pool.execute_with_retries(show_create_query)

                if not (result_sets and result_sets[0].rows and result_sets[0].rows[0]):
                    logger.error(f"[{thread_name}] SHOW CREATE VIEW `{self.view_path}` returned no data.")
                    show_had_critical_error = True
                else:
                    create_query_column_index = -1
                    for i, col in enumerate(result_sets[0].columns):
                        if col.name == "CreateQuery":
                            create_query_column_index = i
                            break
                    if create_query_column_index == -1:
                        logger.error(
                            f"[{thread_name}] Column 'CreateQuery' not found in SHOW CREATE VIEW result for {self.view_path}."
                        )
                        show_had_critical_error = True
                    else:
                        create_query = result_sets[0].rows[0][create_query_column_index]
                        logger.debug(f"[{thread_name}] SHOW CREATE VIEW `{self.view_path}` result:\n{create_query}")

                        expected_view_substr = f"CREATE VIEW `{self.view_path}`"
                        expected_table_substr = f"FROM\n    `{self.table_path}`"

                        if not (expected_view_substr in create_query and expected_table_substr in create_query):
                            logger.error(
                                f"[{thread_name}] VALIDATION FAILED for `{self.view_path}`:\n"
                                f"  Expected view part: '{expected_view_substr}'\n"
                                f"  Expected table part: '{expected_table_substr}'\n"
                                f"  Got:\n{create_query}"
                            )
                            show_had_critical_error = True
                        else:
                            logger.debug(f"[{thread_name}] SHOW CREATE VIEW `{self.view_path}` validated.")
                            show_was_successful = True
            except ydb.SchemeError:
                logger.debug(
                    f"[{thread_name}] SHOW CREATE VIEW `{self.view_path}` failed as expected (view likely gone)."
                )
            except Exception as e:
                logger.warning(f"[{thread_name}] SHOW CREATE VIEW `{self.view_path}` failed with other error: {e}")
                show_had_critical_error = True

            if select_was_successful and show_was_successful:
                self._increment_successful_cycles()
            elif select_had_critical_error or show_had_critical_error:
                self._increment_failed_cycles()
            else:
                self._increment_no_signal_cycles()

        logger.info(f"[{thread_name}] stopped.")

    def loop(self):
        logger.info(f"Starting workload loop for {self.duration} seconds...")

        recreation_thread = threading.Thread(target=self._recreation_loop, name="Recreation-Thread")
        showing_thread = threading.Thread(target=self._showing_loop, name="Showing-Thread")

        recreation_thread.start()
        showing_thread.start()

        main_start_time = time.time()
        while time.time() - main_start_time < self.duration:
            if not recreation_thread.is_alive() or not showing_thread.is_alive():
                logger.warning("A worker thread stopped prematurely.")
                self._mark_overall_workload_error()
                break
            time.sleep(0.2)

        logger.info("Duration reached or thread stopped. Signaling stop event.")
        self._stop_event.set()

        recreation_thread.join(timeout=5)
        showing_thread.join(timeout=5)

        if recreation_thread.is_alive():
            error_msg = "Recreation thread did not join in time."
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        if showing_thread.is_alive():
            error_msg = "Showing thread did not join in time."
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.info(
            f"Final showing loop stats: successful cycles = {self.successful_cycles}, failed cycles = {self.failed_cycles}, no signal cycles = {self.no_signal_cycles}"
        )

        if self.overall_workload_error_occurred:
            logger.error("Workload finished with critical errors.")
        elif self.failed_cycles > 0:
            logger.error(f"Workload finished with {self.failed_cycles} failed showing cycles.")
        elif self.successful_cycles == 0:
            logger.warning("Workload completed with zero showing cycles attempted or recorded.")

    def __enter__(self):
        try:
            self.preliminary_setup()
        except Exception as e:
            logger.critical(f"Preliminary setup failed, cannot start workload: {e}")
            try:
                self.__exit__(type(e), e, e.__traceback__)
            except Exception as exit_e:
                logger.error(f"Exception during __exit__ after setup failure: {exit_e}")
            raise
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        logger.info("Exiting workload...")
        if not self._stop_event.is_set():
            logger.info("Signaling threads to stop during exit.")
            self._stop_event.set()

        self.cleanup_resources()
        self.pool.stop()
        self.driver.stop()
        logger.info("Workload stopped.")
