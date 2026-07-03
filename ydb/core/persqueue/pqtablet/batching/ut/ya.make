UNITTEST_FOR(ydb/core/persqueue/pqtablet/batching)

SRCS(
    batch_cutter_ut.cpp
)

PEERDIR(
    library/cpp/streams/zstd
    ydb/core/persqueue/public/write_meta
    ydb/core/protos
    ydb/public/sdk/cpp/src/library/kafka
)

END()
