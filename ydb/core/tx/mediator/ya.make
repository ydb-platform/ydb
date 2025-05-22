LIBRARY()

SRCS(
    mediator.cpp
    mediator_impl.cpp
    mediator__init.cpp
    mediator__configure.cpp
    mediator__schema.cpp
    mediator__schema_upgrade.cpp
    tablet_queue.cpp
    execute_queue.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/scheme_types
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/tx/coordinator/public
    ydb/core/tx/time_cast
    ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
