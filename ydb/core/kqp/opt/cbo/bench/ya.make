LIBRARY()

SRCS(
    kqp_benches.cpp
    kqp_join_topology_generator.cpp
)

PEERDIR(
    library/cpp/disjoint_sets
    library/cpp/json
    library/cpp/iterator
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

YQL_LAST_ABI_VERSION()

END()
