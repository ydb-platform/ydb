LIBRARY()

SRCS(
    res_or_pull.cpp
    table_limiter.cpp
)

PEERDIR(
    library/cpp/yson
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/providers/common/codec
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/lib/mkql_helpers
)

YQL_LAST_ABI_VERSION()

END()
