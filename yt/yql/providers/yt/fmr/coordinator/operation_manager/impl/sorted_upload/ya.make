LIBRARY()

SRCS(
    yql_yt_sorted_upload_stage_operation_manager.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base
    yt/yql/providers/yt/fmr/coordinator/operation_manager/interface
    yt/yql/providers/yt/fmr/coordinator/partitioner
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/interface
    yql/essentials/utils
    yql/essentials/utils/log
)

END()
