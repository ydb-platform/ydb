LIBRARY()

SRCS(
    alter_local_index.cpp
    alter_store.cpp
    alter_table.cpp
    create_local_index.cpp
    create_table_with_local_indexes.cpp
    create_store.cpp
    create_table.cpp
    drop_local_index.cpp
    drop_store.cpp
    move_local_index.cpp
    drop_table.cpp
    prepare_index_validation.cpp
    read_only_copy_table.cpp
)

PEERDIR(
    ydb/core/mind/hive
    ydb/services/bg_tasks
    ydb/core/tx/schemeshard/olap/operations/alter
)

YQL_LAST_ABI_VERSION()

END()
