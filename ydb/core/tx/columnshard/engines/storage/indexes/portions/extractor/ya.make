LIBRARY()


SRCS(
    abstract.cpp
    GLOBAL sub_column.cpp
    GLOBAL default.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/protos
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
)

YQL_LAST_ABI_VERSION()

END()
