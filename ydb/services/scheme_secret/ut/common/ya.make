LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/table
    ydb/library/aclib
    ydb/core/kqp/common/events
    ydb/core/kqp/ut/common
    ydb/services/scheme_secret
)

YQL_LAST_ABI_VERSION()

END()
