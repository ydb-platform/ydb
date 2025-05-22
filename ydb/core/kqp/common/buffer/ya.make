LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/kqp/common/simple
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()
