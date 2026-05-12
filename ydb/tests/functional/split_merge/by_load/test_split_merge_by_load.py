import concurrent.futures
import enum
import json
import logging
import typing as tp
import urllib.parse
import uuid

import attr
import pytest
import requests

from ydb.tests.library.common.types import TabletTypes
from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.library.fixtures.safe_parametrize import ParameterSet, safe_mark_parametrize
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels
from ydb.tests.oss.ydb_sdk_import import ydb


_logger = logging.getLogger(__name__)
"""
The logger used in all tests in this file.
"""


_TENANT_DB_NAME: tp.Final[str] = "tenant_db"
"""
The name of the tenant database used by all the tests.
"""


_TABLE_NAME: tp.Final[str] = "test_table"
"""
The name of the table used by all the tests.
"""


_ROW_COUNT: tp.Final[int] = 100000
"""
The total number of rows added to the test table by these tests.
"""


_DATA_COLUMN_COUNT: tp.Final[int] = 5
"""
The number of data columns (with random data) created by these tests.
"""


_ROW_BATCH_SIZE: tp.Final[int] = 100
"""
The number of rows, which are updated in the test table in a single batch.
"""


_READ_ROW_COUNT: tp.Final[int] = 1000
"""
The number of rows read from the test table to produce the load for splitting.
"""


_READ_THREAD_COUNT: tp.Final[int] = 50
"""
The number of parallel threads used to produce the load for splitting.
"""


_MAX_PARTITION_COUNT: tp.Final[int] = 5
"""
The maximum number of partitions allowed for the split-by-load logic.
"""


@attr.s(auto_attribs=True, slots=True, frozen=True, kw_only=True)
class _YdbTestContext():
    """
    The context for executing a test, which holds the YDB connection.
    """

    cluster: KiKiMR
    """
    The controller for the YDB cluster started for the tests.
    """

    driver: ydb.Driver
    """
    The client driver for the test database.
    """

    session_pool: ydb.QuerySessionPool
    """
    The client session pool for the test database.
    """

    db_http_endpoint: str
    """
    The HTTP endpoint for the test database (monitoring).

    NOTE: When using a tenant DB, this HTTP endpoint points to the dynamic node,
          which runs the tenant database.
    """

    database: str
    """
    The name of the test database.
    """


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = {
    "additional_log_configs": {
        # Increase the verbosity of DataShard and SchemeShard logs
        "TX_DATASHARD": LogLevels.TRACE,
        "FLAT_TX_SCHEMESHARD": LogLevels.TRACE,
        "TABLET_RESOLVER": LogLevels.TRACE,
        "STATESTORAGE": LogLevels.DEBUG,
    },
    "datashard_config": {
        # NOTE: Reduce the interval at which periodic stats are sent to SchemeShard
        #       so that the split/merge by load logic is triggered every second.
        #       Also reduce the frequence at which the key access sample can be
        #       collected and sent back to SchemeShard.
        "stats_report_interval_seconds": 1,
        "key_access_sample_collection_min_interval_seconds": 4,
        "key_access_sample_collection_max_interval_seconds": 10,
        "key_access_sample_validity_interval_seconds": 60,
    },
}
"""
The local configuration for the YDB cluster (picked up by the ydb_cluster_configuration fixture).
"""


@enum.unique
class _DatabaseType(enum.StrEnum):
    """
    The type of of the database created by the _ydb_context_for_test fixture().
    """
    ROOT_DB = "root_db"
    TENANT_DB = "tenant_db"


@pytest.fixture(
    scope="function",
    params=[
        _DatabaseType.ROOT_DB,
        _DatabaseType.TENANT_DB,
    ],
)
def _ydb_context_for_test(
    ydb_cluster: KiKiMR,
    ydb_client: tp.Callable[[str], ydb.Driver],
    request: pytest.FixtureRequest,
) -> _YdbTestContext:
    """
    The fixture, which creates all the necessary pieces to run the tests in this file.

    :param ydb_cluster: The runner for the YDB cluster
    :type ydb_cluster: KiKiMR

    :param ydb_client: The helper function, which creates a driver for the given database
    :type ydb_client: tp.Callable[[str], ydb.Driver]

    :param request: The invocation context for this fixture
    :type request: pytest.FixtureRequest

    :returns: The context for executing the test
    :rtype: _YdbTestContext
    """

    # Reduce the logging level for ydb.connection to avoid printing queries
    logging.getLogger("ydb.connection").setLevel(logging.INFO)

    match request.param:
        case _DatabaseType.ROOT_DB:
            # Use the root database for the tests
            database_path = "/Root"
            driver = ydb_client("/Root")

            with ydb.QuerySessionPool(
                driver,
                workers_threads_count=_READ_THREAD_COUNT,
            ) as session_pool:
                yield _YdbTestContext(
                    cluster=ydb_cluster,
                    driver=driver,
                    session_pool=session_pool,
                    db_http_endpoint="http://{}:{}".format(
                        ydb_cluster.nodes[1].host,
                        ydb_cluster.nodes[1].mon_port,
                    ),
                    database=database_path,
                )

        case _DatabaseType.TENANT_DB:
            # Create a new tenant database for the tests
            with ydb_database_ctx(
                ydb_cluster,
                "/Root/{}".format(_TENANT_DB_NAME),
            ) as tenant_db_path:
                driver = ydb_client(tenant_db_path)

                with ydb.QuerySessionPool(
                    driver,
                    workers_threads_count=_READ_THREAD_COUNT,
                ) as session_pool:
                    yield _YdbTestContext(
                        cluster=ydb_cluster,
                        driver=driver,
                        session_pool=session_pool,
                        # NOTE: This must point to the dynamic node, which is allocated
                        #       for the tenant database created above
                        db_http_endpoint="http://{}:{}".format(
                            ydb_cluster.slots[1].host,
                            ydb_cluster.slots[1].mon_port,
                        ),
                        database=tenant_db_path,
                    )

        case _:
            assert False, "Invalid database type"


def _create_test_table(context: _YdbTestContext) -> None:
    """
    Create the test table.

    :param context: The context for executing the test
    :type context: _YdbTestContext
    """
    # Create a table with followers
    #
    # NOTE: All splitting (both by size and by load) is disabled by now
    #       to avoid the table being splitted by the initial bulk insert load
    _logger.info(
        "Creating the test table %s/%s",
        context.database,
        _TABLE_NAME,
    )

    context.session_pool.execute_with_retries(
        """
CREATE TABLE `{}`(
id Uint64 NOT NULL,
{}
PRIMARY KEY(id)
)
WITH (
AUTO_PARTITIONING_BY_SIZE = DISABLED,
AUTO_PARTITIONING_BY_LOAD = DISABLED,
AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = {},
READ_REPLICAS_SETTINGS = "ANY_AZ:1"
);
        """.format(
            _TABLE_NAME,
            "\n".join(
                "    value{} Utf8 NOT NULL,".format(column_index)
                for column_index in range(_DATA_COLUMN_COUNT)
            ),
            _MAX_PARTITION_COUNT,
        ),
    )


def _populate_test_table(context: _YdbTestContext) -> None:
    """
    Populate the test table with some random data (using UPSERT).

    :param context: The context for executing the test
    :type context: _YdbTestContext
    """
    _logger.info("Populating the table with random data")

    # Add a whole bunch of rows with random data splitting them into batches
    column_types = ydb.BulkUpsertColumns()
    column_types.add_column("id", ydb.PrimitiveType.Uint64)

    for column_index in range(_DATA_COLUMN_COUNT):
        column_types.add_column(
            "value{}".format(column_index),
            ydb.PrimitiveType.Utf8,
        )

    for i in range(_ROW_COUNT // _ROW_BATCH_SIZE):
        _logger.info(
            "Adding %s rows, batch %s out of %s",
            _ROW_BATCH_SIZE,
            i + 1,
            _ROW_COUNT // _ROW_BATCH_SIZE,
        )

        context.driver.table_client.bulk_upsert(
            "{}/{}".format(context.database, _TABLE_NAME),
            [
                {
                    "id" : i * _ROW_BATCH_SIZE + j,
                } | {
                    "value{}".format(column_index): str(uuid.uuid4())
                    for column_index in range(_DATA_COLUMN_COUNT)
                }
                for j in range(_ROW_BATCH_SIZE)
            ],
            column_types,
        )


def _enable_split_by_load_for_test_table(
    context: _YdbTestContext,
    for_merging: bool,
) -> None:
    """
    Enable the split-by-load logic for the test table.

    :param context: The context for executing the test
    :type context: _YdbTestContext

    :param for_merging: If true, the parameters are high low to allow merging (for slitting otherwise)
    :type for_merging: bool
    """

    # Enable splitting-by-load for the test table
    _logger.info("Enabling split-by-load for the test table")

    context.session_pool.execute_with_retries(
        """
ALTER TABLE `{}` SET AUTO_PARTITIONING_BY_LOAD ENABLED;
        """.format(
            _TABLE_NAME,
        ),
    )

    # Configure split-by-load parameters to allow very fast splits
    icb_url = "{}/actors/icb".format(context.db_http_endpoint)

    _logger.info("Configuring split-by-load settings via ICB: %s", icb_url)

    for icb_key, icb_value in {
        # The CPU load percentage at which the table is split
        #
        # WARNING: The absolute minimum CPU load percentage used by SchemeShard
        #          to evaluate the merge-by-load conditions is 2%. In other words,
        #          SchemeShard always uses at least 2% CPU load when trying
        #          to add a partition to the merge set. Since the overall CPU load
        #          threshold is 70% of the value below, this value must be set
        #          to at least 6% (which allows up to 2 partitions to be merged,
        #          more precisely: 6% * 70% = 4.2% > 2 * 2%).
        #
        #          The real problem is that the Python code is not capable of producing
        #          enough load to force the table to be split, if the splitting threshold
        #          is set high enough to allow automatic merging. Thus, there must be
        #          two separate limits: one for splitting (low) and another for merging (high).
        "SchemeShardControls.FastSplitCpuPercentageThreshold": (50 if for_merging else 1),

        # The high CPU load duration after which the table is merged back
        "SchemeShardControls.MergeByLoadMinLowLoadDurationSec": 10,

        # The minimum partition uptime after which the table can be split
        "SchemeShardControls.MergeByLoadMinUptimeSec": 1,
    }.items():
        requests.post(
            icb_url,
            data="{}={}".format(icb_key, icb_value),
        ).raise_for_status()


@attr.s(auto_attribs=True, slots=True, frozen=True, kw_only=True)
class _TableStats():
    """
    The overall statistics collected for the given table.
    """

    row_count: int
    data_size: int
    partition_count: int


def _get_test_table_stats(context: _YdbTestContext) -> _TableStats:
    """
    Retrieve the basic statistics for the test table.

    :param context: The context for executing the test
    :type context: _YdbTestContext

    :returns: The statistics for the test table
    :rtype: _TableStats
    """
    describe_url = "{}/viewer/json/describe?{}".format(
        context.db_http_endpoint,
        urllib.parse.urlencode({
            "database": context.database,
            "path": "{}/{}".format(context.database, _TABLE_NAME),
            "enums": "true",
            "partition_stats": "true",
            "subs": "0",
        }),
    )

    _logger.info(
        "Collecting the statistics for the test table: %s",
        describe_url,
    )

    response = requests.get(describe_url)
    response.raise_for_status()

    json_table_stats = response.json()["PathDescription"]["TableStats"]

    table_stats = _TableStats(
        row_count=int(json_table_stats["RowCount"]),
        data_size=int(json_table_stats["DataSize"]),
        partition_count=int(json_table_stats["PartCount"]),
    )

    _logger.info(
        "The test table contains %s row(s) (data size: %s), %s partition(s)\n%s",
        table_stats.row_count,
        table_stats.data_size,
        table_stats.partition_count,
        json.dumps(response.json(), sort_keys=True, indent=4),
    )

    return table_stats


def _make_read_load(
    session: ydb.query.pool.SimpleQuerySessionCheckout,
    read_mode: ydb.BaseQueryTxMode,
) -> None:
    """
    Generate SELECT transactions to induce some read load on leaders/followers.

    :param session: The YDB session to use
    :type session: ydb.query.pool.SimpleQuerySessionCheckout

    :param read_mode: The read mode to use (for example, ydb.QueryStaleReadOnly())
    :type read_mode: ydb.BaseQueryTxMode
    """
    _logger.info(
        "Reading %s rows in the '%s' mode to generate some load on the test table",
        _READ_ROW_COUNT,
        read_mode.name,
    )

    # Read the given number of rows, but spread them evenly to hit all partitions
    for i in range(_READ_ROW_COUNT):
        with session.transaction(read_mode).execute(
            """
SELECT * FROM `{}` WHERE id = {};
            """.format(
                _TABLE_NAME,
                i * (_ROW_COUNT // _READ_ROW_COUNT),
            ),
            commit_tx=True,
        ):
            # NOTE: The result of the transaction is a stream, which must be closed,
            #       the context manager automatically enumerates till the end
            #       and forces the stream to be closed
            pass


def _wait_for_tablet_resolver_cache(context: _YdbTestContext) -> None:
    """
    Wait until the Tablet Resolver cache becomes populated with all followers.

    :param context: The context for executing the test
    :type context: _YdbTestContext
    """
    _logger.info("Waiting for the Tablet Resolver cache to become populated with followers")

    # Find the tablet ID for the DataShard for the test table
    #
    # NOTE: Since there is only one table created for the test, there should be
    #       only one DataShard present
    tablet_state_response = context.cluster.client.tablet_state(TabletTypes.FLAT_DATASHARD)

    leader_tablet_ids = [
        info.TabletId
        for info in tablet_state_response.TabletStateInfo
        if info.Leader
    ]

    assert len(leader_tablet_ids) == 1, "There should be exactly one leader DataShard"

    follower_url = "{}/tablets/app?TabletID={}&FollowerID=1".format(
        context.db_http_endpoint,
        leader_tablet_ids[0]
    )

    _logger.info(
        "The first DataShard tabletId=%s, follower URL: %s",
        leader_tablet_ids[0],
        follower_url,
    )

    def _check_follower_page() -> bool:
        response = requests.get(follower_url)
        response.raise_for_status()

        follower_ready = (" is not connected " not in response.text)

        _logger.info(
            "The Tablet Resolver cache for tablet ID %s is %s",
            leader_tablet_ids[0],
            "ready" if follower_ready else "not ready",
        )

        return follower_ready

    wait_completed = wait_for(
        _check_follower_page,
        timeout_seconds=20.0,
        step_seconds=0.1,
        multiply=1,
    )

    assert wait_completed, "The Tablet Resolver cache is not ready"


def _get_followers_read_counters(context: _YdbTestContext) -> int:
    """
    Calculate how many read operations all followers processed.

    :param context: The context for executing the test
    :type context: _YdbTestContext
    """

    # Read .sys/partition_stats for the test table
    result = context.session_pool.execute_with_retries(
        """
SELECT Path,
       PartIdx,
       TabletId,
       FollowerId,
       CPUCores,
       RowCount,
       DataSize,
       RowReads,
       RangeReads,
       RangeReadRows
FROM `.sys/partition_stats`
WHERE Path = '{}/{}';
        """.format(
            context.database,
            _TABLE_NAME,
        ),
    )

    _logger.info(
        "The current .sys/partition_stats data for the table '%s':\n%s",
        _TABLE_NAME,
        "\n".join(str(row) for row in result[0].rows),
    )

    # Count reads for all followers (no followers == no reads!)
    all_follower_reads = 0

    for row in result[0].rows:
        if row["FollowerId"] != 0:
            all_follower_reads += row["RowReads"]
            all_follower_reads += row["RangeReads"]
            all_follower_reads += row["RangeReadRows"]

    return all_follower_reads


# NOTE: There are 2 variations for this test: leaders and followers. In each variation
#       the test applies the read load only on the leader (or followers) of each partition.
#       The test expects the table to be split multiple times, once the leaders (or followers)
#       become sufficiently overloaded. When the table is split the expected number
#       of times, the load on the leaders (or followers) is turned off and the test expects
#       the table to be merged back into a single partition.
@safe_mark_parametrize(
    ParameterSet("leader", read_mode=ydb.QueryOnlineReadOnly()),
    ParameterSet("follower", read_mode=ydb.QueryStaleReadOnly()),
)
def test_split_merge_by_load(
    _ydb_context_for_test: _YdbTestContext,
    *,
    read_mode: ydb.BaseQueryTxMode,
) -> None:
    """
    Verify that the split/merge by load logic works with the load on the leader/follower.

    :param _ydb_context_for_test: The context for executing the test
    :type _ydb_context_for_test: _YdbTestContext

    :param read_mode: The read mode to use (for example, ydb.QueryStaleReadOnly())
    :type read_mode: ydb.BaseQueryTxMode
    """

    # Create/populate the test table and enable split-by-load
    _create_test_table(_ydb_context_for_test)
    _populate_test_table(_ydb_context_for_test)
    _enable_split_by_load_for_test_table(_ydb_context_for_test, for_merging=False)

    # Make sure the table has not been split yet
    #
    # NOTE: The table statistics are updated asynchronously.
    #       Thus, there needs to be a loop here with a relatively long wait time
    def _check_table_row_count() -> bool:
        table_stats = _get_test_table_stats(_ydb_context_for_test)

        assert table_stats.partition_count == 1
        return table_stats.row_count == _ROW_COUNT

    wait_completed = wait_for(
        _check_table_row_count,
        timeout_seconds=30.0,
        step_seconds=0.1,
        multiply=1,
    )

    assert wait_completed, "The test table does not contain all the expected rows"

    # NOTE: The Tablet Resolver cache is populated asynchronously. In the beginning,
    #       it may not contain any followers. Thus, trying to send read requests
    #       to followers will result in the load being put on the leader instead.
    #       The wait here keeps on pinging the developer UI page for one of the followers,
    #       which cause the Tablet Resolver to update its cache (if the follower is not found).
    #
    # WARNING: On top of the Tablet Resolver cache, there is another cache
    #          in the pipe-per-node cache, which is used by KQP. Once the pipe-per-node
    #          cache is populated, there is no good way to reset it. If there is any SELECT
    #          executed with the stale_read_only mode before this point in the test,
    #          the pipe-per-node cache will use an incorrect Tablet Resolver cache
    #          and will establish a connection to the leader even in the stale_read_only mode.
    #          After this ALL such requests will go to the leader instead of going
    #          to the followers.
    _wait_for_tablet_resolver_cache(_ydb_context_for_test)

    # Make sure no reads from followers have happened so far
    all_follower_reads = _get_followers_read_counters(_ydb_context_for_test)
    assert all_follower_reads == 0, "There should not be any reads from followers"

    # Start producing the load on the leader/follower and wait for the table to split
    _logger.info("Starting the load on the test table and waiting for the table to be split")

    def _produce_load_and_check_splitting() -> bool:
        # Read from multiple parallel threads to make the load higher
        result = concurrent.futures.wait(
            [
                _ydb_context_for_test.session_pool.retry_operation_async(
                    lambda session: _make_read_load(session, read_mode),
                )
                for _ in range(_READ_THREAD_COUNT)
            ],
        )

        for completed_future in result.done:
            completed_future.result()  # Throws, if there was an exception

        # NOTE: As a condition of completing the wait use both the partition count
        #       and the total number of rows. The partition count should change
        #       after the table is split. And the correct row count indicates
        #       that all statistics have been collected asynchronously after the split.
        table_stats = _get_test_table_stats(_ydb_context_for_test)

        return (
            (table_stats.partition_count == _MAX_PARTITION_COUNT)
            and (table_stats.row_count == _ROW_COUNT)
        )

    wait_completed = wait_for(
        _produce_load_and_check_splitting,
        timeout_seconds=300.0,
        step_seconds=0.1,
        multiply=1,
    )

    assert wait_completed, "The test table was not split by the induced load"

    # Make sure there were some reads from followers (if reading from followers)
    #
    # WARNING: Due to the uncertainties related to the pipe-per-node cache
    #          and the Tablet Resovler cache related to the follower discovery
    #          (see above), this test can reliably send reads to followers only
    #          during the first split iteration when the test can guarantee
    #          that stale_read_only requests are directed to the followers.
    #          Once the table starts splitting, there is no way to guarantee that
    #          and there is a chance that some stale_read_only requests will go
    #          to the corresponding leaders.
    if isinstance(read_mode, ydb.QueryOnlineReadOnly):
        all_follower_reads = _get_followers_read_counters(_ydb_context_for_test)
        assert all_follower_reads == 0, "There should not be any reads from followers"

    # Increase the splitting threshold to a high number to allow automatic merging
    _enable_split_by_load_for_test_table(_ydb_context_for_test, for_merging=True)

    # Stop producing the load and wait for the table to be merged back
    _logger.info("Stopping the load on the test table and waiting for the table to be merged")

    def _check_table_partition_count() -> bool:
        # NOTE: As a condition of completing the wait use both the partition count
        #       and the total number of rows. The partition count should change
        #       after the table is split. And the correct row count indicates
        #       that all statistics have been collected asynchronously after the split.
        table_stats = _get_test_table_stats(_ydb_context_for_test)

        return (
            (table_stats.partition_count == 1)
            and (table_stats.row_count == _ROW_COUNT)
        )

    wait_completed = wait_for(
        _check_table_partition_count,
        timeout_seconds=300.0,
        step_seconds=0.1,
        multiply=1,
    )

    assert wait_completed, "The test table was not merged without the induced load"
