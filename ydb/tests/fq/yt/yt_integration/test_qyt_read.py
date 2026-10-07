"""Read YT message streams through the YT provider."""

import logging
import random
import string

import pytest
import ydb

from .yt_in_docker.yt_client import YtClient
from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def yt():
    """Start YT cluster in Docker (managed by docker_compose recipe)."""
    client = YtClient()
    yield client
    client.stop()


@pytest.fixture(scope="module")
def kikimr(request, yt):
    """Start YDB cluster with YT as available external data source.

    The QYT gateway now works in EDS-driven mode: connection parameters
    (endpoint, auth) are taken directly from the EDS properties
    (LOCATION, AUTH_METHOD) rather than from server-side config.

    SQL queries use full absolute YT paths (e.g. ``//tmp/my_queue``) directly
    in the table reference, so no PATH_PREFIX is needed on the EDS.

    Depends on ``yt`` fixture to ensure the YT cluster is available before
    YDB starts.
    """
    param = getattr(request, "param", {})
    set_test_env(request)

    config = get_ydb_config(request)
    config.yaml_config["feature_flags"]["enable_qyt"] = param.get("enable_qyt", True)

    # Register YT as available external data source.
    # No server-side cluster config is needed — the QYT gateway creates
    # YT clients directly from the EDS LOCATION property.
    config.yaml_config["query_service_config"]["available_external_data_sources"].append("YT")

    kikimr = Kikimr(config, enable_discovery=param.get("enable_discovery", True))
    kikimr.qyt_enabled = param.get("enable_qyt", True)
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name():
    """Generate unique entity names to avoid collisions between parallel tests."""
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return wrapper


@pytest.mark.parametrize("kikimr,partition_count,row_count,streaming", [
    ({"enable_qyt": True}, 1, 0, False),
    ({"enable_qyt": True}, 1, 5, False),
    ({"enable_qyt": True}, 3, 5, False),
    ({"enable_qyt": True}, 1, 0, True),
    ({"enable_qyt": False}, 1, 0, False),
], indirect=["kikimr"])
def test_read_queue_via_federated_sql(yt: YtClient, kikimr: Kikimr, entity_name, partition_count, row_count, streaming) -> None:
    """Read every retained row, including empty queues and multiple partitions."""
    logger.info("=== TEST START ===")
    queue_name = entity_name("queue")
    queue_path = f"//tmp/{queue_name}"
    consumer_path = f"//tmp/{entity_name('consumer')}"
    eds_name = entity_name("eds")

    logger.info(f"Queue path: {queue_path}")
    logger.info(f"Consumer path: {consumer_path}")
    logger.info(f"EDS name: {eds_name}")
    logger.info(f"YT proxy URL: {yt.proxy_url}")
    logger.info(f"YT RPC proxy address: {yt.rpc_proxy_address}")

    try:
        # --- 0. Verify YDB is responsive ---
        logger.info("STEP 0: Verifying YDB is responsive...")
        kikimr.ydb_client.query("SELECT 1")
        logger.info("STEP 0: DONE - YDB is responsive")

        # --- 1. Create YT queue ---
        logger.info(f"STEP 1: Creating YT queue at {queue_path}...")
        yt.create_queue(queue_path, data_column="data", tablet_count=partition_count)
        assert yt.exists(queue_path), f"Queue {queue_path} should exist after creation"
        logger.info("STEP 1: DONE - Queue created")

        # --- 2. Write known data ---
        logger.info("STEP 2: Writing known data to queue...")
        test_rows = [
            {"data": "hello"},
            {"data": "world"},
            {"data": "qyt"},
            {"data": "gateway"},
            {"data": "test"},
        ]
        test_rows = test_rows[:row_count]
        for index, row in enumerate(test_rows):
            row["$tablet_index"] = index % partition_count
        yt.insert_rows(queue_path, test_rows)
        logger.info("STEP 2: DONE - Data written")

        # --- 3. Create and register consumer ---
        logger.info(f"STEP 3: Creating queue consumer at {consumer_path}...")
        yt.create_queue_consumer(consumer_path)
        logger.info("STEP 3a: Consumer created, registering...")
        yt.register_consumer(queue_path, consumer_path, vital=True)
        logger.info("STEP 3b: Consumer registered, mounting table...")
        yt.mount_table(consumer_path, sync=True)
        logger.info("STEP 3: DONE - Consumer mounted")

        # --- 5. Create EDS in YDB pointing to YT (no PATH_PREFIX) ---
        logger.info(f"STEP 5: Creating EDS `{eds_name}` in YDB...")
        # Use the RPC proxy address (host:port). The HTTP proxy URL (http://host:PORT)
        # would cause discovery to return internal Docker RPC addresses (unreachable from host).
        # Setting LOCATION = 'host:port' lets CreateYtClient use ProxyAddresses
        # directly (bypassing HTTP discovery).
        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
                SOURCE_TYPE = 'YT',
                LOCATION = '{yt.rpc_proxy_address}',
                AUTH_METHOD = 'NONE'
            );
        """)
        logger.info("STEP 5: DONE - EDS created")

        # --- 6. Execute federated SELECT with absolute YT path ---
        logger.info(f"STEP 6: Executing federated SELECT FROM `{eds_name}`.`{queue_path}`...")
        # Specify CONSUMER so the QYT read session knows which consumer offset to use.
        # No LIMIT: completion must come from MessageStream snapshot bounds.
        if not kikimr.qyt_enabled:
            # With QYT disabled, the native table provider rejects queue-specific settings.
            with pytest.raises(ydb.issues.GenericError, match=r'Unknown setting.*consumer'):
                kikimr.ydb_client.query(f"""
                    SELECT * FROM `{eds_name}`.`{queue_path}`
                    WITH (CONSUMER='{consumer_path}', FORMAT='raw')
                """)
            return

        if streaming:
            output_topic = entity_name("output")
            query_name = entity_name("streaming_query")
            kikimr.ydb_client.query(f"CREATE TOPIC `{output_topic}`")
            try:
                with pytest.raises(ydb.issues.GenericError, match="Reading from data source yt is not supported now for streaming queries"):
                    kikimr.ydb_client.query(f"""
                        CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                            INSERT INTO `{output_topic}`
                            SELECT Data FROM `{eds_name}`.`{queue_path}`
                            WITH (CONSUMER='{consumer_path}', FORMAT='raw');
                        END DO;
                    """)
            finally:
                kikimr.ydb_client.query(f"DROP TOPIC `{output_topic}`")
            return

        result = kikimr.ydb_client.query(f"""
            SELECT * FROM `{eds_name}`.`{queue_path}`
            WITH (CONSUMER='{consumer_path}', FORMAT='raw')
        """)
        logger.info("STEP 6: DONE - Query executed")

        # --- 7. Verify results match written data ---
        logger.info("STEP 7: Verifying results...")
        # The query() method returns a list of result sets from execute_with_retries
        # Each result set has .rows which are protobuf rows with .items access
        all_rows = []
        if isinstance(result, list):
            # execute_with_retries returns list of result sets
            for rs in result:
                all_rows.extend(rs.rows)
        else:
            all_rows = result.rows

        actual_data = sorted([
            row["Data"].decode('utf-8') if isinstance(row, dict) and "Data" in row
            else row.get("data", "") if isinstance(row, dict)
            else str(row.items[0].text_value, 'utf-8')
            for row in all_rows
        ])
        expected_data = sorted([row["data"] for row in test_rows])
        logger.info(f"STEP 7: Actual data: {actual_data}")
        logger.info(f"STEP 7: Expected data: {expected_data}")
        assert actual_data == expected_data, (
            f"Federated SQL returned {actual_data}, expected {expected_data}"
        )
        logger.info("=== TEST PASSED ===")

    finally:
        # --- Cleanup ---
        try:
            yt.remove(consumer_path)
        except Exception:
            logger.warning("Failed to cleanup consumer %s", consumer_path, exc_info=True)
        try:
            yt.remove(queue_path)
        except Exception:
            logger.warning("Failed to cleanup queue %s", queue_path, exc_info=True)
