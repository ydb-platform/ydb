LIBRARY()

SRCS(
    yt_url_lister.cpp
)

PEERDIR(
    library/cpp/cgiparam
    yql/essentials/core/url_lister/interface
    yt/yql/providers/yt/lib/init_yt_api
    yql/essentials/utils/fetch
    yql/essentials/utils/log
    yt/cpp/mapreduce/interface
)

END()
