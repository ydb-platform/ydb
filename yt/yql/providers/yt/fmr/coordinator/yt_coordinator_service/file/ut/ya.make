UNITTEST()

SRCS(
    yql_yt_coordinator_service_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/file
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/utils/comparator
)

YQL_LAST_ABI_VERSION()

END()
