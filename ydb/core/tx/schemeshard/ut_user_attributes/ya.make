UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/http/io
    library/cpp/http/server
    library/cpp/svnversion
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/blockstore/core
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/datashard
    ydb/core/tx/schemeshard
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/util
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_user_attributes.cpp
)

END()
