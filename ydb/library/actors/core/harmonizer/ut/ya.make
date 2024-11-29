UNITTEST_FOR(ydb/library/actors/core/harmonizer)

FORK_SUBTESTS()
IF (SANITIZER_TYPE)
    SIZE(MEDIUM)
    TIMEOUT(600)
ELSE()
    SIZE(SMALL)
    TIMEOUT(60)
ENDIF()


PEERDIR(
    ydb/library/actors/interconnect
    ydb/library/actors/testlib
)

SRCS(
    harmonizer_ut.cpp
)

END()
