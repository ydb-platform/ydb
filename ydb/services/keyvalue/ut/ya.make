UNITTEST_FOR(ydb/services/keyvalue)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
<<<<<<< HEAD
=======
    library/cpp/threading/future
    ydb/core/blobstorage/base
>>>>>>> b92ff4a0b95 ([YDBBUGS-1075] (+) Add deterministic KV proxy status regression coverage (#59019))
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/keyvalue
)

YQL_LAST_ABI_VERSION()

END()
