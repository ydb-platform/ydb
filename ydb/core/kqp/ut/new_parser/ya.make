# Keep compatibility checks, strict canonical join-order comparisons and
# direct workload operator checks separate: success in the fallback suite
# does not establish successful native lowering.
RECURSE_FOR_TESTS(
    fallback
    join_order
    rbo
    workloads
)
