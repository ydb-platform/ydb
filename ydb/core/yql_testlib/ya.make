LIBRARY()

SRCS(
    yql_testlib.cpp
    yql_testlib.h
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/client
    ydb/core/client/minikql_compile
    ydb/core/client/server
    ydb/core/engine
    ydb/core/keyvalue
    ydb/core/mind
    ydb/core/protos
    ydb/core/testlib/default
    ydb/core/testlib/actors
    ydb/core/testlib/basics
    yql/essentials/core/facade
    yql/essentials/public/udf
    ydb/public/lib/base
    yql/essentials/core
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
)

YQL_LAST_ABI_VERSION()

END()
