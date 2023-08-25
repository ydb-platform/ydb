UNITTEST_FOR(ydb/core/fq/libs/compute/common)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    config_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
