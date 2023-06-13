LIBRARY()

PEERDIR(
    library/cpp/http/io
    library/cpp/openssl/io
    library/cpp/string_utils/url
    library/cpp/uri
)

SRCS(
    http_client.cpp
)

END()

IF (NOT OS_WINDOWS)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
