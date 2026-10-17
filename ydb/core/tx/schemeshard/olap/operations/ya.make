YQL_LIBRARY()

SRCS(
    alter_local_index.cpp  # gn: into ydb/core/tx/schemeshard
    alter_store.cpp  # gn: into ydb/core/tx/schemeshard
    alter_table.cpp  # gn: into ydb/core/tx/schemeshard
    alter_table_with_local_indexes.cpp  # gn: into ydb/core/tx/schemeshard
    create_local_index.cpp  # gn: into ydb/core/tx/schemeshard
    create_table_with_local_indexes.cpp  # gn: into ydb/core/tx/schemeshard
    create_store.cpp  # gn: into ydb/core/tx/schemeshard
    create_table.cpp  # gn: into ydb/core/tx/schemeshard
    drop_local_index.cpp  # gn: into ydb/core/tx/schemeshard
    drop_store.cpp  # gn: into ydb/core/tx/schemeshard
    drop_table.cpp  # gn: into ydb/core/tx/schemeshard
    drop_table_with_local_indexes.cpp  # gn: into ydb/core/tx/schemeshard
    move_local_index.cpp  # gn: into ydb/core/tx/schemeshard
    read_only_copy_table.cpp  # gn: into ydb/core/tx/schemeshard
)

PEERDIR(
    ydb/core/mind/hive
    ydb/services/bg_tasks
    ydb/core/tx/schemeshard/olap/operations/alter
)

END()
