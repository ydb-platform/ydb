LIBRARY()

SRCS(
    yql_yt_pull_stage_operation_manager.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base
    yql/essentials/utils
    yql/essentials/utils/log
)

END()
