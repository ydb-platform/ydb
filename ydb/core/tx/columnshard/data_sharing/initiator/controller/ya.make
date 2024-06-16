LIBRARY()

SRCS(
    abstract.cpp
    GLOBAL test.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/initiator/status
    ydb/services/bg_tasks/abstract
)

END()
