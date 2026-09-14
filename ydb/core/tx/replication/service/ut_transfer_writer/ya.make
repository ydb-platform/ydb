UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/tx/datashard/ut_common
    ydb/core/tx/replication/ut_helpers
    ydb/core/tx/replication/ydb_proxy
    library/cpp/string_utils/base64
    library/cpp/testing/unittest
)

SRCS(
    transfer_writer_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
