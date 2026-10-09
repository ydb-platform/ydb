PROGRAM(dq_hash_combine_perf)

# Benchmark tool, intended for manual runs only.

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/comp_nodes/ut/utils
    library/cpp/getopt/small
)

SRCS(
    main.cpp
)

END()
