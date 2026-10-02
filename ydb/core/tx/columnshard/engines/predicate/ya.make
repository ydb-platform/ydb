LIBRARY()

SRCS(
    container.cpp
    range.cpp
    filter.cpp
    predicate.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/portions
    ydb/core/formats/arrow
    ydb/core/formats/arrow/filter
)

YQL_LAST_ABI_VERSION()

END()
