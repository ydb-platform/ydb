LIBRARY()

SRCS(
    events.cpp
    probes.cpp
    quoter_resource_tree.cpp
    quoter_runtime.cpp
    rate_accounting.cpp
    schema.cpp
    tablet_db.cpp
    tablet_html.cpp
    tablet_impl.cpp
    tablet.cpp
    tx_config_get.cpp
    tx_config_set.cpp
    tx_dummy.cpp
    tx_init_schema.cpp
    tx_init.cpp
    tx_quoter_resource_add.cpp
    tx_quoter_resource_delete.cpp
    tx_quoter_resource_describe.cpp
    tx_quoter_resource_update.cpp
    tx_self_check.cpp
    tx_semaphore_acquire.cpp
    tx_semaphore_create.cpp
    tx_semaphore_delete.cpp
    tx_semaphore_describe.cpp
    tx_semaphore_release.cpp
    tx_semaphore_timeout.cpp
    tx_semaphore_update.cpp
    tx_session_attach.cpp
    tx_session_destroy.cpp
    tx_session_detach.cpp
    tx_session_timeout.cpp
    tx_sessions_describe.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    ydb/core/actorlib_impl
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/erasure
    ydb/core/metering
    ydb/core/protos
    ydb/core/tablet_flat
)

END()

RECURSE(
    quoter_performance_test
)

RECURSE_FOR_TESTS(
    ut
)
