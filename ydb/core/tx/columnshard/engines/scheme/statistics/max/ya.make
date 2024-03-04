LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL operator.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/core/tx/schemeshard/olap/schema
    ydb/core/formats/arrow
)

END()
