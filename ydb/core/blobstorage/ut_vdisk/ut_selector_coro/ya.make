UNITTEST()

FORK_SUBTESTS()
SIZE(MEDIUM)
REQUIREMENTS(cpu:4 ram:4)

SRCS(
    selector_equivalence_ut.cpp
)

PEERDIR(
    ydb/apps/version
    ydb/core/blobstorage/vdisk/hulldb
    ydb/core/blobstorage/vdisk/hulldb/test
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()

RECURSE_FOR_TESTS(bench)
