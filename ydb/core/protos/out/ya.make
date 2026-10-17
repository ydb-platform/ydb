LIBRARY()

SRCS(
    out.cpp  # gn: into ydb/core/protos
    out_cms.cpp  # gn: into ydb/core/protos
    out_data_events.cpp  # gn: into ydb/core/protos
    out_long_tx_service.cpp  # gn: into ydb/core/protos
    out_sequenceshard.cpp  # gn: into ydb/core/protos
    out_tablet.cpp  # gn: into ydb/core/protos
)

PEERDIR(
    ydb/core/protos
)

END()
