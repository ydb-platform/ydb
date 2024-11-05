LIBRARY()

SRCS(
    local.cpp
    query_executor.cpp
    typed_local.cpp
    writer.cpp
    get_value.cpp
    aggregation.cpp
)

PEERDIR(
    ydb/core/testlib
)

YQL_LAST_ABI_VERSION()

END()
