from __future__ import annotations

from ydb.tests.stability.nemesis.internal.nemesis import cluster_context


def test_use_cluster_is_visible_and_restores_previous_override():
    outer = object()
    inner = object()

    with cluster_context.use_cluster(outer):
        assert cluster_context.require_cluster() is outer
        with cluster_context.use_cluster(inner):
            assert cluster_context.require_cluster() is inner
        assert cluster_context.require_cluster() is outer


def test_local_harness_starts_with_one_explicitly_allowed_type():
    assert cluster_context.nemesis_rejection_reason("NetworkNemesis") is None

    with cluster_context.use_cluster(object()):
        assert cluster_context.nemesis_rejection_reason("KillDataShardNemesis") is None
        reason = cluster_context.nemesis_rejection_reason("NetworkNemesis")
        assert reason is not None
        assert "not enabled for local harness execution" in reason
