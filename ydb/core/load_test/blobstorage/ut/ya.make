UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    pdisk_log_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/util
    ydb/core/blobstorage/pdisk/mock
    ydb/core/load_test/blobstorage
    ydb/core/util/actorsys_test
)

END()
