LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    GLOBAL operator.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/statistics/abstract
    ydb/core/tx/columnshard/engines/scheme/abstract
    ydb/core/tx/columnshard/splitter/abstract
    ydb/core/formats/arrow
)

END()
