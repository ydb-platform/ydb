LIBRARY()

SRCS(
    yqlrun_lib.cpp
)

PEERDIR(
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/gateway/file

    yql/essentials/providers/common/provider
    yql/essentials/core/cbo
    yql/essentials/core/peephole_opt
    yql/essentials/core/cbo/simple
    yql/essentials/core/services

    yql/essentials/tools/yql_facade_run

)

YQL_LAST_ABI_VERSION()

END()
