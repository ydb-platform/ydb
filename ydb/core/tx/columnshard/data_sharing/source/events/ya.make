LIBRARY()

SRCS(
    transfer.cpp
    control.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/data_sharing/source/session
)

END()
