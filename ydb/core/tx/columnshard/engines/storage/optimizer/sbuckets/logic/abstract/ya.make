LIBRARY()

SRCS(
    logic.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets/common
    ydb/core/tx/columnshard/engines/portions
    ydb/core/formats/arrow
)

END()
