LIBRARY()

SRCS(
    request.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/minikql
    ydb/library/yql/providers/common/mkql
)

YQL_LAST_ABI_VERSION()

END()
