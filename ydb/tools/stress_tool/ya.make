PROGRAM(ydb_stress_tool)


PEERDIR(
    library/cpp/getopt
    ydb/apps/version
    ydb/core/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/lwtrace_probes
    ydb/core/blobstorage/pdisk
    ydb/core/load_test
    ydb/core/node_whiteboard
    ydb/core/tablet
    ydb/library/pdisk_io
    ydb/library/yql/parser/pg_wrapper
    ydb/library/yql/sql/pg
    ydb/library/yql/minikql/comp_nodes/llvm14
    ydb/tools/stress_tool/lib
    ydb/tools/stress_tool/proto
)

SRCS(
    device_test_tool.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
