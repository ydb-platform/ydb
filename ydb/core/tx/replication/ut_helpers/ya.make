LIBRARY()

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/testlib/default
    library/cpp/testing/unittest
)

SRCS(
    test_env.h
)

YQL_LAST_ABI_VERSION()

END()
