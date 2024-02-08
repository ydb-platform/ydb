UNITTEST_FOR(ydb/library/actors/http)

SIZE(SMALL)

PEERDIR(
    ydb/library/actors/testlib
)

IF (NOT OS_WINDOWS)
SRCS(
    http_ut.cpp
)
ELSE()
ENDIF()

END()
