LIBRARY()

SRCS(
    ytrun_lib.cpp
)

PEERDIR(
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/lib/config_clusters
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/lib/yt_url_lister
    yt/yql/providers/yt/lib/log

    yql/essentials/providers/common/provider
    yql/essentials/core/cbo
    yql/essentials/core/peephole_opt
    yql/essentials/core/cbo/simple
    yql/essentials/core/services
    yql/essentials/utils/backtrace
    yql/essentials/tools/yql_facade_run

    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/interface

    library/cpp/digest/md5
    library/cpp/malloc/api
    library/cpp/sighandler
)

YQL_LAST_ABI_VERSION()

END()
