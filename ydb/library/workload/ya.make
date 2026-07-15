LIBRARY()

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/clickbench
    ydb/library/workload/fulltext
    ydb/library/workload/kv
    ydb/library/workload/log
    ydb/library/workload/mixed
    ydb/library/workload/query
    ydb/library/workload/stock
    ydb/library/workload/tpcc
    ydb/library/workload/tpcds
    ydb/library/workload/tpch
    ydb/library/workload/vector
)

END()

RECURSE(
    abstract
    benchmark_base
    clickbench
    fulltext
    kv
    log
    mixed
    query
    stock
    tpcc
    tpc_base
    tpcds
    tpch
    vector
)
