LIBRARY()

SRCS(
    abstract.cpp
    alter.cpp
    alter_impl.cpp
    table_record.cpp
    restore.cpp
    modification.cpp
    generic_manager.cpp
    preparation_controller.cpp
    restore_controller.cpp
    common.cpp
    ydb_value_operator.cpp
    modification_controller.cpp
    object.cpp
    fetch_database.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/table_creator
    ydb/public/api/protos
    ydb/core/protos
    ydb/services/bg_tasks/abstract
    ydb/services/metadata/initializer
    ydb/core/base
    ydb/services/metadata/request
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
