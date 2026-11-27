UNITTEST_FOR(ydb/services/config)

SIZE(MEDIUM)

SRCS(
    bsconfig_ut.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/threading/future
    ydb/core/base
    ydb/core/blobstorage/base
    ydb/core/blobstorage/nodewarden
    ydb/core/protos
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/library/actors/testlib
    ydb/library/yaml_config/public
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/services/dynamic_config
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(cpu:1)
END()
