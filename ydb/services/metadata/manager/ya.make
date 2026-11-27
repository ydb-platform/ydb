YQL_LIBRARY()

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
    contrib/libs/apache/arrow
    library/cpp/retry
    library/cpp/threading/future
    ydb/core/base
    ydb/core/protos
    ydb/core/scheme_types
    ydb/core/tx/locks
    ydb/library/accessor
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/conclusion
    ydb/library/services
    ydb/library/table_creator
    ydb/public/api/protos
    ydb/public/lib/scheme_types
    ydb/services/bg_tasks/abstract
    ydb/services/metadata/abstract
    ydb/services/metadata/initializer
    ydb/services/metadata/request
    yql/essentials/sql/settings
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
