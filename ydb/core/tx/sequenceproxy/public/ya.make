LIBRARY()

SRCS(
    events.cpp
)

PEERDIR(
    ydb/core/base
    ydb/library/aclib
    ydb/public/api/protos
    ydb/library/yql/public/issue
)

YQL_LAST_ABI_VERSION()

END()
