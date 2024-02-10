LIBRARY()

SRCS(
    context.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/library/actors/core
    ydb/library/conclusion
)

END()
