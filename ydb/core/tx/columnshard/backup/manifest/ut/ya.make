UNITTEST_FOR(ydb/core/tx/columnshard/backup/manifest)

SIZE(SMALL)

PEERDIR(
    ydb/core/backup/common
    ydb/core/testlib/default
    ydb/core/tx/datashard
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_manifest.cpp
)

END()
