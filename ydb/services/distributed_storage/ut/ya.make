UNITTEST_FOR(ydb/services/distributed_storage)

SIZE(MEDIUM)

IF (OS_LINUX AND BUILD_TYPE == "RELWITHDEBINFO")
    # The linked test exceeds the DWARF32 relocation range.
    LDFLAGS(-Wl,--strip-debug)
ENDIF()

SRCS(
    distributed_storage_ut.cpp
)

PEERDIR(
    library/cpp/logger
    library/cpp/threading/future
    ydb/core/blobstorage/base
    ydb/core/formats
    ydb/core/protos
    ydb/core/testlib/default
    ydb/public/api/grpc/draft
    ydb/services/distributed_storage
)

YQL_LAST_ABI_VERSION()

END()
