LIBRARY()

SRCS(
    txallocator__reserve.cpp
    txallocator__scheme.cpp
    txallocator_impl.cpp
    txallocator.cpp
)

PEERDIR(
    ydb/library/actors/helpers
    ydb/library/actors/interconnect
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
