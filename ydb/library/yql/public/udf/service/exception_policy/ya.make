LIBRARY()

OWNER(
    a-romanov
    g:yql
    g:yql_ydb_core
)

PROVIDES(YqlServicePolicy)

SRCS(
    udf_service.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
)

YQL_LAST_ABI_VERSION()

END()
