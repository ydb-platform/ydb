LIBRARY()

SRCS(
    cert_auth_utils.cpp
    cert_auth_processor.cpp
    cert_check.cpp
)


PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/public/lib/ydb_cli/common
    contrib/libs/openssl
)

END()

RECURSE_FOR_TESTS(
    ut
)
