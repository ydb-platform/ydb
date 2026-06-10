UNITTEST_FOR(ydb/core/persqueue/pqtablet/batching)

SRCS(
    batch_cutter_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/public/write_meta
    ydb/core/protos
    ydb/library/kafka
)

END()
