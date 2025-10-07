LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    test_http_server.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/http/misc
)

END()
