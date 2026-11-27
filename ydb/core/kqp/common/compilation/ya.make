YQL_LIBRARY()

SRCS(
    result.cpp
    events.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/lwtrace
    ydb/core/kqp/common/simple
    ydb/core/protos
    ydb/core/util
    ydb/library/aclib
    ydb/library/actors/core
    yql/essentials/ast
    yql/essentials/public/issue
)

END()
