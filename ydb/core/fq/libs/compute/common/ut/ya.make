UNITTEST_FOR(ydb/core/fq/libs/compute/common)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    config_ut.cpp
    utils_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    resources/plan.json      plan.json
    resources/stat.json      stat.json
)

END()
