UNITTEST_FOR(ydb/public/lib/ydb_cli/topic)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    topic_write.h
    topic_write.cpp
    topic_write_ut.cpp
    topic_read_ut.cpp
)

PEERDIR(
    library/cpp/histogram/hdr
    library/cpp/threading/local_executor
    ydb/core/fq/libs/private_client
    ydb/public/sdk/cpp/client/ydb_persqueue_public
    ydb/public/lib/experimental
    ydb/public/lib/ydb_cli/commands
    ydb/public/lib/ydb_cli/common
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/client/ydb_proto
    ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

END()
