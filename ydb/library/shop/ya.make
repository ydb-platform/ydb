LIBRARY()

SRCS(
    probes.cpp
    shop.cpp
    flowctl.cpp
)

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/lwtrace
    ydb/library/shop/protos
    library/cpp/deprecated/atomic
)

END()

RECURSE(
    sim_flowctl
    sim_shop
    ut
)
