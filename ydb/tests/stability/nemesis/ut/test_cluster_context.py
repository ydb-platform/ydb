from __future__ import annotations

from types import SimpleNamespace

from ydb.tests.stability.nemesis.internal.nemesis import cluster_context


def test_use_cluster_is_visible_and_restores_previous_override():
    outer = object()
    inner = object()

    with cluster_context.use_cluster(outer):
        assert cluster_context.require_cluster() is outer
        with cluster_context.use_cluster(inner):
            assert cluster_context.require_cluster() is inner
        assert cluster_context.require_cluster() is outer


def test_local_harness_uses_explicit_manual_and_scheduled_allowlists():
    assert cluster_context.nemesis_rejection_reason("NetworkNemesis") is None

    with cluster_context.use_cluster(object()):
        assert cluster_context.nemesis_rejection_reason("KillDataShardNemesis") is None
        for manual_only_type in ("KillHiveNemesis", "KillSchemeShardNemesis"):
            assert cluster_context.nemesis_rejection_reason(manual_only_type) is None
            assert (
                cluster_context.scheduled_nemesis_rejection_reason(manual_only_type)
                is not None
            )
        assert cluster_context.nemesis_rejection_reason("KillDataShardNemesis") is None
        assert (
            cluster_context.scheduled_nemesis_rejection_reason(
                "KillDataShardNemesis"
            )
            is None
        )
        for process_type in (
            "KillNodeNemesis",
            "KillSlotDaemonNemesis",
            "StopStartNodeNemesis",
            "SuspendNodeNemesis",
        ):
            assert cluster_context.nemesis_rejection_reason(process_type) is None
            assert (
                "requires at least 8 block-4-2 nodes"
                in cluster_context.scheduled_nemesis_rejection_reason(
                    process_type
                )
            )
        assert (
            "dedicated non-default-kind spare PDisk per node"
            in cluster_context.scheduled_nemesis_rejection_reason(
                "SafelyBreakDiskNemesis"
            )
        )
        reason = cluster_context.nemesis_rejection_reason("NetworkNemesis")
        assert reason is not None
        assert "not enabled for local harness execution" in reason

    safe_cluster = SimpleNamespace(
        config=SimpleNamespace(static_erasure="block-4-2"),
        nodes={node_id: object() for node_id in range(1, 9)},
    )
    with cluster_context.use_cluster(safe_cluster):
        for process_type in (
            "KillNodeNemesis",
            "StopStartNodeNemesis",
            "SuspendNodeNemesis",
        ):
            assert (
                cluster_context.scheduled_nemesis_rejection_reason(process_type)
                is None
            )
        assert (
            "requires live local harness slots"
            in cluster_context.scheduled_nemesis_rejection_reason(
                "KillSlotDaemonNemesis"
            )
        )
        assert (
            "dedicated non-default-kind spare PDisk per node"
            in cluster_context.scheduled_nemesis_rejection_reason(
                "SafelyBreakDiskNemesis"
            )
        )

    safe_cluster.slots = {1: object(), 2: object()}
    with cluster_context.use_cluster(safe_cluster):
        assert (
            cluster_context.scheduled_nemesis_rejection_reason(
                "KillSlotDaemonNemesis"
            )
            is None
        )

    safe_cluster.config.pdisks_info = [
        {
            "node_id": node_id,
            "pdisk_id": pdisk_id,
            "pdisk_user_kind": user_kind,
        }
        for node_id in safe_cluster.nodes
        for pdisk_id, user_kind in ((1, 0), (2, 1))
    ]
    with cluster_context.use_cluster(safe_cluster):
        assert (
            cluster_context.scheduled_nemesis_rejection_reason(
                "SafelyBreakDiskNemesis"
            )
            is None
        )
