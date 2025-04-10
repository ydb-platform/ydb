LIBRARY()

SRCS(
    database_resolver.cpp
    db_async_resolver_impl.cpp
    http_proxy.cpp
    mdb_endpoint_generator.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/threading/future
    ydb/core/fq/libs/common
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/events
    ydb/core/util
    ydb/library/actors/core
    ydb/library/actors/http
    ydb/library/services
    ydb/library/yql/providers/common/db_id_async_resolver
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/dq/actors
    yql/essentials/utils
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)

