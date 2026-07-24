LIBRARY()

SRCS(
    audit_config.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/library/aclib/protos/identity
)

END()

RECURSE(
    fuzz_targets
)

RECURSE_FOR_TESTS(
    ut
)
