LIBRARY()

SRCS(
    request.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/minikql
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/core
    ydb/library/yql/sql
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(request.h)

END()
