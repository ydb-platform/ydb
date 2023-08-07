LIBRARY()

SRCS(
    yt_url_lister.cpp
)

PEERDIR(
    library/cpp/cgiparam
    ydb/library/yql/core/url_lister/interface
    ydb/library/yql/providers/yt/lib/init_yt_api
    ydb/library/yql/utils/log
    yt/cpp/mapreduce/interface
)

END()
