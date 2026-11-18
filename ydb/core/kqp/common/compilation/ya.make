YQL_LIBRARY()

SRCS(
    result.cpp
    events.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/kqp/query_data
    ydb/core/kqp/common/simple
    yql/essentials/public/issue
)

END()
