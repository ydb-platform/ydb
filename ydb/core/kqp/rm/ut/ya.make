UNITTEST_FOR(ydb/core/kqp/rm)

OWNER(g:kikimr)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_resource_estimation_ut.cpp
    kqp_rm_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
