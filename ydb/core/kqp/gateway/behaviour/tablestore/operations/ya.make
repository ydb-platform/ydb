LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL add_column.cpp
    GLOBAL alter_column.cpp
    GLOBAL drop_column.cpp
    GLOBAL upsert_index.cpp
    GLOBAL drop_index.cpp
    GLOBAL upsert_stat.cpp
    GLOBAL drop_stat.cpp
    GLOBAL upsert_opt.cpp
    GLOBAL alter_sharding.cpp
)

PEERDIR(
    ydb/services/metadata/manager
    ydb/core/formats/arrow/serializer
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/core/kqp/gateway/utils
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
