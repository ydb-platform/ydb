LIBRARY()

SRCS(
    mirror_describer.cpp
    partition_scale_request.cpp
    partition_scale_manager.cpp
    partition_scale_manager_graph_cmp.cpp
    read_balancer__balancing_app.cpp
    read_balancer__balancing.cpp
    read_balancer__mlp_balancing.cpp
    read_balancer_app.cpp
    read_balancer.cpp
)

GENERATE_ENUM_SERIALIZATION(read_balancer__balancing.h)

PEERDIR(
    contrib/libs/fmt
    ydb/core/engine/minikql
    ydb/core/persqueue/events
    ydb/core/persqueue/common
)

END()

RECURSE_FOR_TESTS(
)
