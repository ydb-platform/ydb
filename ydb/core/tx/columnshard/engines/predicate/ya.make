LIBRARY()

SRCS(
    container.cpp
    range.cpp
    filter.cpp
    predicate.cpp
    system_columns_filter.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/scheme
    ydb/core/ydb_convert
    ydb/core/tx/columnshard/engines/portions
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
)

YQL_LAST_ABI_VERSION()

END()
