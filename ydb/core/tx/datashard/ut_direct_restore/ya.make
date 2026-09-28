UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/streams/zstd
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/datashard
    ydb/core/tx/datashard/ut_common
    ydb/core/wrappers/ut_helpers
    ydb/library/aws_init
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/result
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

SRCS(
    datashard_ut_direct_restore.cpp
)

END()
