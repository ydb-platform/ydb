PEERDIR(
    contrib/libs/librdkafka/src-cpp
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/topic
)

ADDINCL(
    contrib/libs/librdkafka/src-cpp
    contrib/libs/librdkafka/include
)

SRCS(
    helpers.cpp
    produce_ut.cpp
    consume_ut.cpp
    balance_ut.cpp
    transactions_ut.cpp
    admin_ut.cpp
    groups_offsets_ut.cpp
    protocol_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16 cpu:4)
ENDIF()
