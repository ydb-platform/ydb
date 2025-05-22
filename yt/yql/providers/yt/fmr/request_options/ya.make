LIBRARY()

SRCS(
    yql_yt_request_options.cpp
)

PEERDIR(
    library/cpp/yson/node
    library/cpp/threading/future
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(yql_yt_request_options.h)

END()

RECURSE(proto_helpers)
