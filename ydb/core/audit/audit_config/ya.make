LIBRARY()

SRCS(
    audit_config.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/aclib/protos/identity
)

END()

RECURSE_FOR_TESTS(
    ut
)
