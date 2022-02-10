LIBRARY()

OWNER(
    ilnaz
    g:kikimr
)

PEERDIR(
    ydb/core/base 
    ydb/core/engine/minikql 
    ydb/core/protos 
    ydb/core/tablet_flat 
    ydb/core/tx/replication/ydb_proxy
    ydb/core/util
    ydb/core/ydb_convert
)

SRCS(
    controller.cpp
    discoverer.cpp
    dst_creator.cpp
    logging.cpp
    private_events.cpp
    replication.cpp
    stream_creator.cpp
    sys_params.cpp
    target_base.cpp
    target_table.cpp
    target_with_stream.cpp
    tx_assign_stream_name.cpp
    tx_create_dst_result.cpp
    tx_create_replication.cpp
    tx_create_stream_result.cpp
    tx_discovery_result.cpp
    tx_drop_replication.cpp
    tx_init.cpp
    tx_init_schema.cpp
)

GENERATE_ENUM_SERIALIZATION(replication.h)

YQL_LAST_ABI_VERSION()

END()
