LIBRARY()

OWNER(
    pg
    mvel
    kulikov
    g:base
    g:middle
)

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
)

END()

RECURSE_FOR_TESTS(ut)
