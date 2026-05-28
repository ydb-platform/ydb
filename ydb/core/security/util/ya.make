LIBRARY()

SRCS(
    counters.cpp
    jwk.cpp
    net.cpp
)

PEERDIR(
    library/cpp/ipv6_address
    library/cpp/json/writer
    library/cpp/monlib/dynamic_counters
    library/cpp/string_utils/base64
    ydb/core/base
    ydb/core/security/certificate_check
    contrib/libs/openssl
)

END()

RECURSE_FOR_TESTS(
    ut
)
