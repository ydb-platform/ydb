import logging
import contextlib
import ydb


def make_driver_config(endpoint, database, path):
    return ydb.DriverConfig(
        endpoint,
        database,
        credentials=ydb.construct_credentials_from_environ(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )


@contextlib.contextmanager
def session_pool_context(
    driver_config: ydb.DriverConfig, size=1, workers_threads_count=1
):
    with ydb.Driver(driver_config) as driver:
        try:
            logging.info("connecting to the database")
            driver.wait(timeout=15)
        except TimeoutError:
            logging.critical(
                f"connection failed\n"
                f"last reported errors by discovery: {driver.discovery_debug_details()}"
            )
            raise
        with ydb.SessionPool(
            driver, size=size, workers_threads_count=workers_threads_count
        ) as session_pool:
            try:
                yield session_pool
            except Exception as e:
                logging.critical(f"failed to create session pool due to {repr(e)}")
