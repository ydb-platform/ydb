LIBRARY()

SRCS(
    kqp_federated_query_actors.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/core/kqp/common/events
    ydb/core/kqp/common/simple
    ydb/core/base
    ydb/core/protos
    ydb/core/util
    ydb/library/aclib
    ydb/library/actors/core
    ydb/library/ycloud/api
    ydb/public/sdk/cpp/src/client/driver
    ydb/services/scheme_secret
    ydb/library/ycloud/api
    ydb/library/ycloud/impl
)

YQL_LAST_ABI_VERSION()

END()
