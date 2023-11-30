LIBRARY()

# See documentation
# https://wiki.yandex-team.ru/kikimr/techdoc/db/cxxapi/

SRCS(
    configurator.cpp
    dynamic_node.cpp
    error.cpp
    kicli.h
    kikimr.cpp
    query.cpp
    result.cpp
    schema.cpp
)

PEERDIR(
    contrib/libs/grpc
    ydb/library/actors/core
    library/cpp/threading/future
    ydb/core/protos
    ydb/library/aclib
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/lib/base
    ydb/public/lib/deprecated/client
    ydb/public/lib/scheme_types
    ydb/public/lib/value
    ydb/library/yql/public/decimal
    ydb/library/yql/public/issue
)

END()

RECURSE_FOR_TESTS(
    ut
)
