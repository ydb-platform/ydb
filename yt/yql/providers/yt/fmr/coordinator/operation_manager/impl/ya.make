LIBRARY()

SRCS(
    yql_yt_default_stage_operation_manager.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/base
    yt/yql/providers/yt/fmr/coordinator/operation_manager/interface
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/upload
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/download
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/merge
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_merge
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/map
    yt/yql/providers/yt/fmr/coordinator/operation_manager/impl/sorted_upload
    yql/essentials/utils
)

END()
