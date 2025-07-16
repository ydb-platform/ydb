GTEST(topic_it)

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/tests/integration/tests_common.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

REQUIREMENTS(ram:32)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/sdk/cpp/src/client/persqueue_public
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/tests/integration/topic/setup
    ydb/public/sdk/cpp/tests/integration/topic/utils
)

YQL_LAST_ABI_VERSION()

SRCS(
    basic_usage.cpp
    describe_topic.cpp
    local_partition.cpp
    topic_to_table.cpp
    trace.cpp
)

END()

RECURSE_FOR_TESTS(
    with_direct_read
)
