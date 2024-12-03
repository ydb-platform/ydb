LIBRARY()

SRCS(
    yql_yt_qplayer_gateway.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/providers/common/schema/expr
    yql/essentials/core
    yql/essentials/core/file_storage
    library/cpp/yson/node
    library/cpp/random_provider
    yt/cpp/mapreduce/interface
    contrib/libs/openssl
)

YQL_LAST_ABI_VERSION()

END()


