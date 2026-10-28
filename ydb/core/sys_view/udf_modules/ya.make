YQL_LIBRARY()

SRCS(
    udf_modules.cpp
)

PEERDIR(
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/query_actor
)

END()
