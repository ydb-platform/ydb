LIBRARY()

SRCS(
    udf_modules.cpp
)

PEERDIR(
    ydb/core/kqp/runtime
    ydb/core/sys_view/common
    ydb/library/actors/core
    ydb/library/query_actor
)

YQL_LAST_ABI_VERSION()

END()
