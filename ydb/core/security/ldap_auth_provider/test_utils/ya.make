LIBRARY()

SRCS(
    test_settings.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/security/certificate_check
)

END()

RECURSE_FOR_TESTS(
    ut
)
