LIBRARY()

SRCS(
    local.cpp
    query_executor.cpp
    typed_local.cpp
    writer.cpp
    get_value.cpp
    aggregation.cpp
    plan_step.cpp
)

PEERDIR(
    ydb/core/testlib
    ydb/core/protos
)

YQL_LAST_ABI_VERSION()

END()
