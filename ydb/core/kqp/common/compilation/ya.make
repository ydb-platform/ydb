YQL_LIBRARY()

SRCS(
    result.cpp
    events.cpp
)

PEERDIR(
    library/cpp/json
    ydb/core/kqp/common/simple
    ydb/core/kqp/query_data
    yql/essentials/public/issue
)

END()
