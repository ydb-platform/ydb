# -*- coding: utf-8 -*-
import logging
import time

from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.stress.common.common import WorkloadBase

logger = logging.getLogger(__name__)


class WorkloadCutHistory(WorkloadBase):
    """Restart ColumnShard tablets so their channel history grows and can be cut.

    CutHistory only has work to do once a tablet's channel history has more than one
    entry, and entries appear on generation changes. Restarting the tablets is the
    cheapest way to produce them from a client; the cutter then nominates and cuts
    the drained ones on its own one-minute cadence.
    """

    def __init__(self, client, prefix, stop, endpoint, period=30):
        super().__init__(client, prefix, "cut_history", stop)
        # kikimr_client_factory speaks plaintext message bus, so grpcs:// cannot work
        # here: reject it instead of stripping the scheme and failing to connect.
        scheme, sep, address = endpoint.rpartition("://")
        if sep and scheme != "grpc":
            raise ValueError(f"cut_history needs a grpc:// endpoint, got {endpoint}")
        host, _, port = address.partition(":")
        self.kikimr_client = kikimr_client_factory(host, port or "2135")
        self.period = period
        self.restarts = 0
        self.errors = 0

    def get_stat(self):
        return f"Restarts: {self.restarts}, Errors: {self.errors}"

    def _column_shard_ids(self):
        response = self.kikimr_client.tablet_state(tablet_type=TabletTypes.COLUMNSHARD)
        return [info.TabletId for info in response.TabletStateInfo]

    def _loop(self):
        while not self.is_stop_requested():
            try:
                tablet_ids = self._column_shard_ids()
                if not tablet_ids:
                    # The other workloads may not have created a column table yet.
                    time.sleep(self.period)
                    continue
                for tablet_id in tablet_ids:
                    if self.is_stop_requested():
                        return
                    self.kikimr_client.tablet_kill(tablet_id)
                    self.restarts += 1
                logger.info("cut_history: restarted %s ColumnShard tablet(s)", len(tablet_ids))
            except Exception as e:
                self.errors += 1
                logger.warning("cut_history: restart round failed: %s", e)
            # Wait longer than the cutter's nomination cadence so a sweep can finish
            # between rounds instead of every entry being reopened by a new generation.
            waited = 0
            while waited < self.period and not self.is_stop_requested():
                time.sleep(1)
                waited += 1

    def get_workload_thread_funcs(self):
        return [self._loop]
