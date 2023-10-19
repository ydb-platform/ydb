LIBRARY()

PEERDIR(
    ydb/library/yql/dq/runtime
)

SRCS(
    counters.cpp
    task_counters.cpp
)


   YQL_LAST_ABI_VERSION()


END()
