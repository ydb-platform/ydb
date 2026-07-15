import logging
import pytest
import random
import string
import time

from ydb.tests.fq.streaming_common.common import Kikimr, YdbClient, get_ydb_config, set_test_env
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
from ydb.tests.library.harness.param_constants import kikimr_driver_path


logger = logging.getLogger(__name__)


class KikimrRollingUpgrade(Kikimr):
    """
    Kikimr cluster with 2 dynamic nodes initially running the *current* binary.

    Provides a :meth:`roll` generator that drives a minimal rolling
    upgrade/downgrade of one dynamic node:

    * Yield 0 (step 1):   roll node1 to stable version.
    * Yield 1 (step 2):   roll node2 to stable version.
    * Yield 2 (step 3):   roll node1 + node2 back to *current* version.

    The YDB client is pinned to the *stable* node so that it remains connected
    across the restarts of the rolling node.
    """

    def __init__(self, config, main_binary_path, stable_binary_path, **kwargs):
        self._main_binary_path = main_binary_path
        self._stable_binary_path = stable_binary_path
        super().__init__(config, **kwargs)

        # Identify the two slot IDs deterministically.
        slot_ids = sorted(self.cluster.slots.keys())
        assert len(slot_ids) >= 2, "KikimrRollingUpgrade requires at least 2 dynamic slots"
        self._rolling_slot_id = slot_ids[0]  # the slot that will be rolled
        self._stable_slot_id = slot_ids[1]   # stays on current version throughout

        stable_node = self.cluster.slots[self._stable_slot_id]
        stable_endpoint = Endpoint(
            f"{stable_node.host}:{stable_node.port}",
            self.endpoint.database,
        )
        self.ydb_client.stop()
        self.ydb_client = YdbClient(
            database=stable_endpoint.database,
            endpoint=f"grpc://{stable_endpoint.endpoint}",
            enable_discovery=False,
        )
        self.ydb_client.wait_connection()
        self.first_node = stable_node
        self.endpoint = stable_endpoint

    def _wait_for_readiness(self, timeout: int = 60) -> None:
        """Spin until the cluster accepts queries (verified via the stable node)."""
        deadline = time.time() + timeout
        last_exc = None
        while time.time() < deadline:
            try:
                self.ydb_client.wait_connection(timeout=5)
                return
            except Exception as exc:
                last_exc = exc
                logger.warning("Readiness check failed: %r", exc)
                time.sleep(2)
        raise TimeoutError(f"Cluster not ready after {timeout}s") from last_exc

    def roll(self):
        logger.info("ROLL666")

        rolling_slot_id = 1
        logger.info(f"roll: step 1 — switching slot {rolling_slot_id} to stable version")
        rolling_node = self.cluster.slots[rolling_slot_id]
        rolling_node.stop()
        rolling_node.binary_path = self._stable_binary_path
        rolling_node.set_log_file_prefix("logfile_stable_")
        rolling_node.start()
        self._wait_for_readiness()
        logger.info(f"roll: step 1 complete")
        yield

        rolling_slot_id = 2
        logger.info(f"roll: step2  — switching slot {rolling_slot_id} to stable version")
        rolling_node = self.cluster.slots[rolling_slot_id]
        rolling_node.stop()
        rolling_node.binary_path = self._stable_binary_path
        rolling_node.set_log_file_prefix("logfile_stable_")
        rolling_node.start()
        self._wait_for_readiness()
        logger.info(f"roll: step 2 complete")
        yield

        logger.info(f"roll: step 3 — switching slots back to main version")
        for rolling_node in self.cluster.slots.values():
            rolling_node.stop()

        for rolling_node in self.cluster.slots.values():
            rolling_node.binary_path = self._main_binary_path
            rolling_node.set_log_file_prefix("logfile_main_restored_")

        for rolling_node in self.cluster.slots.values():
            rolling_node.start()
            self._wait_for_readiness()
        logger.info("roll: step 3 complete")
        yield


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper


@pytest.fixture(scope="module")
def kikimr(request):

    from ydb.tests.library.compatibility.fixtures import (
        init_stable_binary_path,
    )

    main_binary_path = kikimr_driver_path()

    logger.info(f"main_binary_path {main_binary_path}")
    logger.info(f"init_stable_binary_path {init_stable_binary_path}")

    set_test_env(request)
    config = get_ydb_config(request)
    config.set_binary_paths([main_binary_path])

    kikimr = KikimrRollingUpgrade(
        config,
        main_binary_path=main_binary_path,
        stable_binary_path=init_stable_binary_path,
    )
    yield kikimr
    kikimr.stop()
