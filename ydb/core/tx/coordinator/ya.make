LIBRARY()

SRCS(
    coordinator.cpp
    coordinator_impl.cpp
    coordinator__acquire_read_step.cpp
    coordinator__configure.cpp
    coordinator__check.cpp
    coordinator__init.cpp
    coordinator__last_step_subscriptions.cpp
    coordinator__mediators_confirmations.cpp
    coordinator__monitoring.cpp
    coordinator__plan_step.cpp
    coordinator__read_step_subscriptions.cpp
    coordinator__restart_mediator.cpp
    coordinator__restore_transaction.cpp
    coordinator__schema.cpp
    coordinator__schema_upgrade.cpp
    coordinator__stop_guard.cpp
    defs.h
    mediator_queue.cpp
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/actors/helpers
    library/cpp/actors/interconnect
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/scheme
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/core/tx
    ydb/core/util
)

END()

RECURSE_FOR_TESTS(
    ut
)
