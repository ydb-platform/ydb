UNITTEST_FOR(ydb/core/mind/address_classification)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    SPLIT_FACTOR(20)
    TAG(ya:fat)
    REQUIREMENTS(ram:16 cpu:1)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:1)
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
