from ydb.apps.dstool.main import main as dstool_main
from ydb.tests.library.common import cms
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.common.wait_for import retry_assertions


STATIC_GROUP_ID = 0
STATIC_ERASURE = Erasure.BLOCK_4_2
STATIC_GROUP_SIZE = STATIC_ERASURE.min_fail_domains

CLUSTER_CONFIG = dict(
    erasure=STATIC_ERASURE,
    nodes=STATIC_GROUP_SIZE + 1,
    use_in_memory_pdisks=False,
    use_config_store=True,
    separate_node_configs=True,
    simple_config=True,
    use_self_management=True,
    extra_grpc_services=['config', 'distributed_storage'],
)


def get_static_vdisks(cluster):
    base_config = cluster.client.query_base_config().BaseConfig
    return [vdisk for vdisk in base_config.VSlot if vdisk.GroupId == STATIC_GROUP_ID]


def vdisk_position(vdisk):
    return (
        vdisk.GroupId,
        vdisk.FailRealmIdx,
        vdisk.FailDomainIdx,
        vdisk.VDiskIdx,
    )


def pdisk_id(vdisk):
    return vdisk.VSlotId.NodeId, vdisk.VSlotId.PDiskId


def format_vdisk_id(vdisk):
    group_id, fail_realm_idx, fail_domain_idx, vdisk_idx = vdisk_position(vdisk)
    return '[%08x:_:%u:%u:%u]' % (group_id, fail_realm_idx, fail_domain_idx, vdisk_idx)


def wait_static_vdisks_ready(cluster):
    def get_ready_vdisks():
        vdisks = get_static_vdisks(cluster)
        assert len(vdisks) == STATIC_GROUP_SIZE, 'Static group is not fully configured'
        assert all(vdisk.Ready for vdisk in vdisks), 'Static group is not BSC-ready'
        return vdisks

    return retry_assertions(get_ready_vdisks, timeout_seconds=180, step_seconds=2)


def wait_vdisk_reassigned(cluster, previous_vdisk):
    position = vdisk_position(previous_vdisk)
    generation = previous_vdisk.GroupGeneration
    source_pdisk = pdisk_id(previous_vdisk)

    def get_reassigned_vdisk():
        candidates = [
            vdisk
            for vdisk in get_static_vdisks(cluster)
            if (
                vdisk_position(vdisk) == position
                and vdisk.GroupGeneration > generation
                and pdisk_id(vdisk) != source_pdisk
            )
        ]
        assert candidates, 'Static VDisk %s was not moved from PDisk %s' % (position, source_pdisk)
        return candidates[0]

    return retry_assertions(get_reassigned_vdisk, timeout_seconds=180, step_seconds=2)


def run_dstool(cluster, *args):
    node = cluster.nodes[1]
    endpoint = 'grpc://%s:%s' % (node.host, node.grpc_port)
    dstool_main(['--endpoint', endpoint, *args])


def test_dstool_evict_static_vdisk(ydb_cluster):
    cms.request_increase_ratio_limit(ydb_cluster.client)
    vdisks = wait_static_vdisks_ready(ydb_cluster)
    previous_vdisk = min(vdisks, key=vdisk_position)

    run_dstool(
        ydb_cluster,
        'vdisk',
        'evict',
        '--vdisk-ids',
        format_vdisk_id(previous_vdisk),
    )

    wait_vdisk_reassigned(ydb_cluster, previous_vdisk)
