LIBRARY()

PEERDIR(
    ydb/library/workload/abstract
    ydb/library/workload/clickbench
    ydb/library/workload/kv
    ydb/library/workload/log_writer
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
    log_writer
    stock
    tpc_base
    tpcds
    tpch
)