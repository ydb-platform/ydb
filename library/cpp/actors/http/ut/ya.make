UNITTEST_FOR(library/cpp/actors/http)

SIZE(SMALL)

PEERDIR(
    library/cpp/actors/testlib
)

IF (NOT OS_WINDOWS)
SRCS(
    http_ut.cpp
)
ELSE()
ENDIF()

END()
