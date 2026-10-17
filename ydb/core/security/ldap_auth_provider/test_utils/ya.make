LIBRARY()

# gn: move into parent

SRCS(
    test_settings.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/security/certificate_check/test_utils
)

END()
