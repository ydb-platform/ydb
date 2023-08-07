LIBRARY()

SRCS(
    defs.h
    events.h
    load_actor_impl.cpp
    load_actor_impl.h
    load_actor_delete.cpp
    load_actor_mon.cpp
    load_actor_read_validate.cpp
    load_actor_state.cpp
    load_actor_write.cpp
    processor.h
    scheme.h
    state_server_interface.cpp
    state_server_interface.h
    test_shard_context.cpp
    test_shard_context.h
    test_shard_impl.h
    test_shard_mon.cpp
    test_tablet.cpp
    test_tablet.h
    tx_init_scheme.cpp
    tx_initialize.cpp
    tx_load_everything.cpp
)

PEERDIR(
    contrib/libs/t1ha
    library/cpp/json
    ydb/core/keyvalue
    ydb/core/protos
    ydb/core/util
)

END()
