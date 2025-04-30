LIBRARY()

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/clickbench
    ydb/library/workload/external
    ydb/library/workload/kv
    ydb/library/workload/log
    ydb/library/workload/mixed
    ydb/library/workload/stock
    ydb/library/workload/tpcds
    ydb/library/workload/tpch
)

END()

RECURSE(
    abstract
    benchmark_base
    clickbench
    external
    kv
    log
    mixed
    stock
    tpc_base
    tpcds
    tpch
)
