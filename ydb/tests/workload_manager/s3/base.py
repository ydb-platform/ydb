from __future__ import annotations

from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class S3WorkloadManagerFunctionalBase(FunctionalTestBase):
    """Hermetic KiKiMR setup for S3 scheduling tests.

    `enable_s3_scheduling` is a class attribute so a test can flip it
    between phases via `_restart_cluster_with_s3_scheduling(bool)`. The
    restart is cluster-only; the caller is responsible for re-provisioning
    pools/users/EDS on the fresh cluster.
    """

    enable_s3_scheduling: bool = True

    @classmethod
    def _cluster_feature_flags(cls) -> list[str]:
        flags = ["enable_external_data_sources"]
        if cls.enable_s3_scheduling:
            flags.append("enable_s3_scheduling")
        return flags

    @classmethod
    def _cluster_query_service_config(cls) -> dict:
        return {
            "available_external_data_sources": ["ObjectStorage"],
            # Per-task S3 read buffer defaults to 200 MiB
            # (TS3GatewayConfig.DataInflight). Under concurrent runners
            # that overshoots the hermetic KiKiMR's memory quota. Use 16 MiB.
            "s3": {
                "data_inflight": 16 * 1024 * 1024,
            },
        }

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster_ext(
            extra_feature_flags=cls._cluster_feature_flags(),
            query_service_config=cls._cluster_query_service_config(),
        )

    @classmethod
    def _restart_cluster_with_s3_scheduling(cls, enable: bool) -> None:
        """Stop the current cluster, start a fresh one with the given flag
        state. Caller must re-provision pools/users/EDS afterwards."""
        cls.cluster.stop()
        cls.enable_s3_scheduling = enable
        cls.setup_cluster_ext(
            extra_feature_flags=cls._cluster_feature_flags(),
            query_service_config=cls._cluster_query_service_config(),
        )
