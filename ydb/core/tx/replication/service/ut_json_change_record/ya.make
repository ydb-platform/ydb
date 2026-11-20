UNITTEST_FOR(ydb/core/tx/replication/service)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/datashard
    library/cpp/json/yson
    library/cpp/string_utils/base64
    ydb/core/testlib/pg
    yql/essentials/public/udf/service/exception_policy
    library/cpp/testing/unittest
)

SRCS(
    json_change_record_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
