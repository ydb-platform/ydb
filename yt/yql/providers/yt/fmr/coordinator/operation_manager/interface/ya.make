LIBRARY()

SRCS(
    yql_yt_stage_operation_manager.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    yt/yql/providers/yt/fmr/request_options
    yql/essentials/utils
)

END()
