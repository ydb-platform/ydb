LIBRARY()

SRCS(
    auth_helpers.cpp
    signature.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/cgiparam
    library/cpp/http/io
    library/cpp/http/misc
    ydb/library/http_proxy/error
)

END()

RECURSE_FOR_TESTS(
    ut
)
