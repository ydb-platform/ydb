UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

TIMEOUT(600)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk
    ydb/core/testlib/default
)

SRCS(
    dsproxy_config_retrieval.cpp
)

YQL_LAST_ABI_VERSION()

END()
