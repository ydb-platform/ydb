LIBRARY()

SRCS(
    status_code_counters.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    ydb/library/yql/dq/actors/protos
)

YQL_LAST_ABI_VERSION()

END()
