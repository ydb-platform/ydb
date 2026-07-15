LIBRARY()

SRCS(
    out.cpp
    out_cms.cpp
    out_data_events.cpp
    out_long_tx_service.cpp
    out_sequenceshard.cpp
    out_tablet.cpp
)

PEERDIR(
    ydb/core/protos
)

END()
