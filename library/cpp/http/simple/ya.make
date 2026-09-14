LIBRARY()

PEERDIR(
    library/cpp/cache
    library/cpp/http/io
    library/cpp/openssl/io
    library/cpp/string_utils/url
    library/cpp/threading/cancellation
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
