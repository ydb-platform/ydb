LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    ydb/core/protos
    ydb/core/tx/conveyor_composite/common
    ydb/library/accessor
    ydb/library/actors/core
    ydb/library/conclusion
)

END()
