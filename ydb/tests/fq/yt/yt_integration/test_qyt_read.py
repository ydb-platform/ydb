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


@pytest.mark.parametrize(
    "kikimr,partition_count,row_count,streaming",
    [
        ({"enable_qyt": True}, 1, 0, False),
        ({"enable_qyt": True}, 1, 5, False),
        ({"enable_qyt": True}, 3, 5, False),
        ({"enable_qyt": True}, 1, 0, True),
        ({"enable_qyt": False}, 1, 0, False),
    ],
    indirect=["kikimr"],
)
def test_read_queue_via_federated_sql(
    yt: YtClient, kikimr: Kikimr, entity_name, partition_count, row_count, streaming
) -> None:
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
        yt.create_queue(queue_path, schema_columns=["{name=data;type_v3=utf8}"], tablet_count=partition_count)
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
                    WITH (CONSUMER='{consumer_path}')
                """)
            return

        if streaming:
            output_topic = entity_name("output")
            query_name = entity_name("streaming_query")
            kikimr.ydb_client.query(f"CREATE TOPIC `{output_topic}`")
            try:
                with pytest.raises(
                    ydb.issues.GenericError,
                    match="Reading from data source yt is not supported now for streaming queries",
                ):
                    kikimr.ydb_client.query(f"""
                        CREATE STREAMING QUERY `{query_name}` AS DO BEGIN
                            INSERT INTO `{output_topic}`
                            SELECT CAST(Unwrap(data) AS String) FROM `{eds_name}`.`{queue_path}`
                            WITH (CONSUMER='{consumer_path}');
                        END DO;
                    """)
            finally:
                kikimr.ydb_client.query(f"DROP TOPIC `{output_topic}`")
            return

        result = kikimr.ydb_client.query(f"""
            SELECT * FROM `{eds_name}`.`{queue_path}`
            WITH (CONSUMER='{consumer_path}')
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

        actual_data = sorted(row["data"] for row in all_rows)
        expected_data = sorted([row["data"] for row in test_rows])
        logger.info(f"STEP 7: Actual data: {actual_data}")
        logger.info(f"STEP 7: Expected data: {expected_data}")
        assert actual_data == expected_data, f"Federated SQL returned {actual_data}, expected {expected_data}"
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


@pytest.mark.parametrize("required_payload", [True, False])
def test_read_queue_with_nullable_and_required_columns(
    yt: YtClient,
    kikimr: Kikimr,
    entity_name,
    required_payload: bool,
) -> None:
    """Check the raw YT rows and the SQL reader with a mixed queue schema."""
    queue_path = f"//tmp/{entity_name('mixed_schema_queue')}"
    consumer_path = f"//tmp/{entity_name('consumer')}"
    eds_name = entity_name("eds")
    payload_type = "utf8" if required_payload else "{type_name=optional;item=utf8}"
    marker_type = "{type_name=optional;item=utf8}" if required_payload else "utf8"

    try:
        yt.create_queue(
            queue_path,
            schema_columns=[
                f"{{name=payload;type_v3={payload_type}}}",
                f"{{name=marker;type_v3={marker_type}}}",
                "{name=id;type=int64;required=%true}",
                "{name=count;type_v3={type_name=optional;item=uint32}}",
            ],
        )
        rows = [
            {"payload": "first", "marker": "present", "id": 1, "count": 7},
            {"payload": "second", "id": 2} if required_payload else {"marker": "present", "id": 2},
        ]
        rows.append(dict(rows[0]))
        yt.insert_rows(queue_path, rows)
        yt.create_queue_consumer(consumer_path)
        yt.register_consumer(queue_path, consumer_path, vital=True)
        yt.mount_table(consumer_path, sync=True)

        pulled = []
        while len(pulled) < len(rows):
            batch = yt.pull_queue_consumer(
                consumer_path,
                queue_path,
                offset=len(pulled),
                max_row_count=len(rows) - len(pulled),
            )
            assert batch, "YT queue returned no rows before all inserted rows were read"
            pulled.extend(batch)
        assert len(pulled) == len(rows)
        assert pulled[0]["payload"] == "first"
        if required_payload:
            assert pulled[1]["payload"] == "second"
            assert pulled[1].get("marker") is None
        else:
            assert pulled[1]["marker"] == "present"
            assert pulled[1].get("payload") is None

        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{eds_name}` WITH (
                SOURCE_TYPE = 'YT',
                LOCATION = '{yt.rpc_proxy_address}',
                AUTH_METHOD = 'NONE'
            );
        """)
        structured = kikimr.ydb_client.query(f"""
            SELECT * FROM `{eds_name}`.`{queue_path}`
            WITH (CONSUMER='{consumer_path}')
        """)
        structured_sets = structured if isinstance(structured, list) else [structured]
        actual_rows = [
            (row["payload"], row["marker"], row["id"], row["count"])
            for result_set in structured_sets
            for row in result_set.rows
        ]
        expected_rows = (
            [("first", "present", 1, 7), ("second", None, 2, None)]
            if required_payload
            else [("first", "present", 1, 7), (None, "present", 2, None)]
        )
        expected_rows.append(expected_rows[0])
        assert actual_rows == expected_rows

        column_positions = {name: index for index, name in enumerate(("payload", "marker", "id", "count"))}
        for projection, columns in [
            ("payload, marker, id, count", ("payload", "marker", "id", "count")),
            ("id, payload", ("id", "payload")),
        ]:
            projected = kikimr.ydb_client.query(f"""
                SELECT {projection} FROM `{eds_name}`.`{queue_path}`
                WITH (CONSUMER='{consumer_path}')
            """)
            projected_sets = projected if isinstance(projected, list) else [projected]
            assert all(tuple(column.name for column in result_set.columns) == columns for result_set in projected_sets)
            assert [
                tuple(row[column] for column in columns) for result_set in projected_sets for row in result_set.rows
            ] == [tuple(row[column_positions[column]] for column in columns) for row in expected_rows]
    finally:
        yt.remove(consumer_path)
        yt.remove(queue_path)
