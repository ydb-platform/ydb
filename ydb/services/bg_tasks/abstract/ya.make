LIBRARY()

SRCS(
    interface.cpp
)

PEERDIR(
    ydb/library/accessor
    ydb/library/actors/core
    ydb/public/api/protos
    ydb/services/bg_tasks/protos
    ydb/library/conclusion
)

END()
