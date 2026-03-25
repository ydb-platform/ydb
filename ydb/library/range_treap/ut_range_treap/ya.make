UNITTEST_FOR(ydb/library/range_treap)

FORK_SUBTESTS()

SPLIT_FACTOR(1)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
)

YQL_LAST_ABI_VERSION()

SRCS(
    range_treap_ut.cpp
)

END()
