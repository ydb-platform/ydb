UNITTEST_FOR(ydb/core/mind/address_classification)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:16)
ENDIF()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    SPLIT_FACTOR(20)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/library/actors/http
    ydb/core/mind/address_classification
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    net_classifier_ut.cpp
)

END()
