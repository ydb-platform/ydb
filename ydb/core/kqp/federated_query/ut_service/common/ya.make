LIBRARY()

SRCS(
    helpers.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/table
    ydb/library/aclib
    ydb/core/kqp/common/events
    ydb/core/kqp/federated_query
    ydb/core/kqp/ut/common
)

YQL_LAST_ABI_VERSION()

END()
