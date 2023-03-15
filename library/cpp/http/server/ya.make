LIBRARY()

SRCS(
    conn.cpp
    http.cpp
    http_ex.cpp
    options.cpp
    response.cpp
)

PEERDIR(
    library/cpp/http/misc
    library/cpp/http/io
    library/cpp/threading/equeue
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
