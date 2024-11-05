LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/kqp/common/simple
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
