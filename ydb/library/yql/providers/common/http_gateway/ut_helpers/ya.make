LIBRARY()

SRCS(
    http_gateway_holder.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/library/yql/providers/common/http_gateway
    ydb/library/yql/utils/actor_log
)

YQL_LAST_ABI_VERSION()

END()
