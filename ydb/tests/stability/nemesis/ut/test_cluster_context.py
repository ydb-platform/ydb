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
