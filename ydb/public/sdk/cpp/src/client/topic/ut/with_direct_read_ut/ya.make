UNITTEST_FOR(ydb/public/sdk/cpp/src/client/topic)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/topic/ut/ut_utils
)

YQL_LAST_ABI_VERSION()

ENV(PQ_EXPERIMENTAL_DIRECT_READ="1")

SRCDIR(
    ydb/public/sdk/cpp/src/client/topic/ut
    ydb/public/sdk/cpp/src/client/topic
)

SRCS(
    basic_usage_ut.cpp
    describe_topic_ut.cpp
    direct_read_ut.cpp
    local_partition_ut.cpp
    topic_to_table_ut.cpp
)

RESOURCE(
    ydb/public/sdk/cpp/src/client/topic/ut/resources/topic_A_partition_0_v24-4-2.dat topic_A_partition_0_v24-4-2.dat
    ydb/public/sdk/cpp/src/client/topic/ut/resources/topic_A_partition_1_v24-4-2.dat topic_A_partition_1_v24-4-2.dat
)

END()
