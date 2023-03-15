LIBRARY()

SRCS(
    txallocator__reserve.cpp
    txallocator__scheme.cpp
    txallocator_impl.cpp
    txallocator.cpp
)

PEERDIR(
    library/cpp/actors/helpers
    library/cpp/actors/interconnect
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
)

END()

RECURSE_FOR_TESTS(
    ut
)
