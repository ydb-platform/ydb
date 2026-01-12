LIBRARY()

SRCS(
    yql_yt_qplayer_gateway.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/yson/node
    library/cpp/random_provider
    yt/cpp/mapreduce/interface
    yql/essentials/providers/common/schema/expr
    yql/essentials/core
    yql/essentials/core/file_storage
    yql/essentials/core/services
    yql/essentials/utils/log
    yt/yql/providers/yt/expr_nodes
    yt/yql/providers/yt/lib/dump_helpers
    yt/yql/providers/yt/lib/full_capture
)

YQL_LAST_ABI_VERSION()

END()
