LIBRARY()

SRCS(
    ic_storage_transport.cpp
)

PEERDIR(
    ydb/core/mind/bscontroller

    ydb/core/nbs/cloud/blockstore/libs/kikimr
)

END()
