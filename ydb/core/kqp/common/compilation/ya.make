LIBRARY()

SRCS(
    result.cpp
    events.cpp
)

PEERDIR(
    ydb/core/kqp/query_data
    ydb/core/kqp/common/simple
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
