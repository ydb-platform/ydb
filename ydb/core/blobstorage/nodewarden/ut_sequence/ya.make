UNITTEST()

OWNER(
    alexvru
    g:kikimr
)

SIZE(MEDIUM)

TIMEOUT(600)

PEERDIR(
    ydb/core/blobstorage/base
    ydb/core/blobstorage/crypto
    ydb/core/blobstorage/nodewarden
    ydb/core/blobstorage/pdisk
    ydb/core/testlib
)

SRCS(
    dsproxy_config_retrieval.cpp
)

YQL_LAST_ABI_VERSION()

END()
