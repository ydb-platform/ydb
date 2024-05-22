# Various tests which we can't run in every pull request (because of instability/specific environment/execution time/etc)

PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLUSTER_YAML="ydb/tests/acceptance/cluster.yaml")

TEST_SRCS(
    test_slice.py
)

TAG(ya:fat)
SIZE(LARGE)

DEPENDS(
    ydb/tests/tools/ydb_serializable
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tools/ydbd_slice
)

DATA(
    arcadia/ydb/tests/acceptance/cluster.yaml
)

END()
