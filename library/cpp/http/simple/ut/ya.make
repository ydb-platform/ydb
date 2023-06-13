UNITTEST_FOR(library/cpp/http/simple)

PEERDIR(
    library/cpp/http/misc
    library/cpp/testing/mock_server
)

SRCS(
    http_ut.cpp
    https_ut.cpp
)

DEPENDS(library/cpp/http/simple/ut/https_server)

DATA(arcadia/library/cpp/http/simple/ut/https_server)

END()

RECURSE(
    https_server
)
