YQL_LIBRARY()

SRCS(
    udf_modules.cpp
)

PEERDIR(
    ydb/core/kqp/common/events
    ydb/core/kqp/runtime/common
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/query_actor
)

END()
