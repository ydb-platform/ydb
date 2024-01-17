LIBRARY()

SRCS(
    yql_testlib.cpp
    yql_testlib.h
)

PEERDIR(
    ydb/library/grpc/client
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
    ydb/library/yql/core/facade
    ydb/library/yql/public/udf
    ydb/public/lib/base
    ydb/library/yql/core
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/udf_resolve
)

YQL_LAST_ABI_VERSION()

END()
