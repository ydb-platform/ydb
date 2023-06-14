LIBRARY()

SRCS(
    out.cpp
    out_cms.cpp
    out_long_tx_service.cpp
    out_sequenceshard.cpp
)

PEERDIR(
    ydb/core/protos
)

END()
