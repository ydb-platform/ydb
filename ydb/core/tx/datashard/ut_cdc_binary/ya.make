UNITTEST_FOR(ydb/core/tx/datashard)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/core/tx/datashard/ut_common
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/streams/zstd
    library/cpp/svnversion
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx
    yql/essentials/public/udf/service/exception_policy
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/result
)

YQL_LAST_ABI_VERSION()

SRCS(
    cdc_binary_compress_ut.cpp
)

END()
