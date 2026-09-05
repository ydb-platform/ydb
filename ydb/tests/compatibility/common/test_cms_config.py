# -*- coding: utf-8 -*-
import pytest

from ydb.tests.library.common.types import Erasure
from ydb.tests.library.compatibility.fixtures import (
    RestartToAnotherVersionFixture,
    current_binary_path,
    current_name,
    inter_stable_binary_path,
    inter_stable_name,
)


current_to_inter_stable = [[current_binary_path, inter_stable_binary_path]]
current_to_inter_stable_ids = [
    "restart_{}_to_{}".format(current_name, inter_stable_name),
]


@pytest.mark.parametrize(
    "base_setup",
    argvalues=current_to_inter_stable,
    ids=current_to_inter_stable_ids,
    indirect=True,
)
class TestCmsConfigDowngrade(RestartToAnotherVersionFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if current_name != "current":
            pytest.skip("requires -DYDB_COMPAT_TARGET_REF=current")

        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            nodes=1,
            cms_config={
                "sentinel_config": {
                    "enable": False,
                    "evict_vdisks_status": "FAULTY",
                    "evict_vdisks_mode": "MAINTENANCE_STATUS",
                },
            },
        )

    def test_maintenance_evict_mode_survives_downgrade(self):
        # The old binary must remain able to start while the YAML written for the
        # current version is still installed. The old version ignores the new
        # mode field and uses evict_vdisks_status as its rollback fallback.
        self.change_cluster_version()
        assert all(node.is_alive() for node in self.cluster.nodes.values())
