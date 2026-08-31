LIBRARY()

SRCS(
    alter_local_index.cpp
    alter_store.cpp
    alter_table.cpp
    alter_table_with_local_indexes.cpp
    create_local_index.cpp
    create_table_with_local_indexes.cpp
    create_store.cpp
    create_table.cpp
    drop_local_index.cpp
    drop_store.cpp
    drop_table.cpp
    drop_table_with_local_indexes.cpp
    move_local_index.cpp
    read_only_copy_table.cpp
)

PEERDIR(
    ydb/core/mind/hive
    ydb/services/bg_tasks
    # schemeshard__affected_paths_traits.h includes the generated op_type_list.h, and ya
    # requires a PEERDIR to the module owning a generated artifact. Not a cycle: generated
    # only peers ydb/core/protos, and this directory is peered *from* schemeshard, not to it.
    ydb/core/tx/schemeshard/generated
    ydb/core/tx/schemeshard/olap/operations/alter
)

YQL_LAST_ABI_VERSION()

END()
