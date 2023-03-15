LIBRARY()

SRCS(
    tasks_packer.cpp
)

PEERDIR(
    ydb/library/yql/dq/proto
    ydb/library/yql/utils
)

YQL_LAST_ABI_VERSION()

END()
