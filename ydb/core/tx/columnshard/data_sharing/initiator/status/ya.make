LIBRARY()

SRCS(
    abstract.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/protos
    ydb/services/bg_tasks/abstract
)

END()
