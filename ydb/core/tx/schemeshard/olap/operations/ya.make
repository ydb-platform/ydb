LIBRARY()

SRCS(
    alter_store.cpp
    alter_table.cpp
    create_store.cpp
    create_table.cpp
    drop_store.cpp
    drop_table.cpp
    read_only_copy_table.cpp
)

PEERDIR(
    ydb/core/mind/hive
    ydb/services/bg_tasks
    ydb/core/tx/schemeshard/olap/operations/alter
)

YQL_LAST_ABI_VERSION()

END()
