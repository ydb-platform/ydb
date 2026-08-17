LIBRARY()

SRCS(
    yql_yt_base_stage_operation_manager.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/operation_manager/interface
    yt/yql/providers/yt/fmr/coordinator/partitioner
    yql/essentials/utils
    yql/essentials/utils/log
)

END()
