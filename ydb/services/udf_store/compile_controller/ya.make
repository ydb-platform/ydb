LIBRARY()

SRCS(
    compile_controller.h
    controller_impl.cpp
    controller_impl.h
    events.h
    private_events.h
    reconcile_actor.cpp
    reconcile_actor.h
    schema.h
    tx_assign.cpp
    tx_finish.cpp
    tx_init.cpp
    tx_init_schema.cpp
    tx_register_worker.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/cms/console
    ydb/core/engine/minikql
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/library/aclib
    ydb/library/actors/core
    ydb/services/metadata
    ydb/services/metadata/request
    ydb/services/udf_store
    ydb/services/udf_store/compile_controller/protos
)

YQL_LAST_ABI_VERSION()

END()
