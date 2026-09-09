PROGRAM(dsproxy_bench)

SRCS(dsproxy_bench.cpp)

PEERDIR(
    library/cpp/json
    ydb/core/blobstorage/dsproxy
    ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

END()
