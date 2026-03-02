UNITTEST_FOR(ydb/core/raw_socket)

SIZE(small)
REQUIREMENTS(cpu:1)
SRCS(
    buffered_writer_ut.cpp
)

PEERDIR(
    ydb/core/raw_socket
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()
END()
