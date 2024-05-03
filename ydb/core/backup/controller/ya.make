LIBRARY()

SRCS(
    tablet.cpp
    tx_init.cpp
    tx_init_schema.cpp
)

PEERDIR(
    library/cpp/lwtrace/protos
    ydb/core/tablet_flat
    ydb/core/protos
    ydb/core/scheme/protos
)

END()
