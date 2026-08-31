from test_canonical_records import CLUSTER_CONFIG as PUBLIC_API_CLUSTER_CONFIG, TOKEN

from helpers import capture_dstool_evict_vdisk_audit


CLUSTER_CONFIG = dict(PUBLIC_API_CLUSTER_CONFIG, extra_grpc_services=[])


def test_dstool_evict_vdisk_legacy_api(ydb_cluster):
    return capture_dstool_evict_vdisk_audit(
        ydb_cluster,
        TOKEN,
        allowed_failure='DisintegratedByExpectedStatus',
    )
