LIBRARY()

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/clickbench
    ydb/library/workload/kv
    ydb/library/workload/log
    ydb/library/workload/stock
    ydb/library/workload/tpcds
    ydb/library/workload/tpch
)

END()

RECURSE(
    abstract
    benchmark_base
    clickbench
    kv
    log
    stock
    tpc_base
    tpcds
    tpch
)