LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL add_column.cpp
    GLOBAL alter_column.cpp
    GLOBAL drop_column.cpp
    GLOBAL upsert_index.cpp
    GLOBAL drop_index.cpp
)

PEERDIR(
    ydb/services/metadata/manager
    ydb/core/formats/arrow/compression
    ydb/core/kqp/gateway/utils
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
