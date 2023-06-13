LIBRARY()

SRCS(
    yql_http_mock_gateway.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/http_gateway
)

YQL_LAST_ABI_VERSION()

END()

