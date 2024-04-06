LIBRARY()

SRCS(
    yql_modules.cpp
    yql_modules.h
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/core
)

YQL_LAST_ABI_VERSION()

END()
