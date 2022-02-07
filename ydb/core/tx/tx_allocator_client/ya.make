LIBRARY()

OWNER(
    ilnaz
    g:kikimr
)

PEERDIR(
    ydb/core/base
    ydb/core/protos
    ydb/core/tablet
    ydb/core/tx/tx_allocator
)

SRCS(
    actor_client.cpp
    client.cpp
)

END()
