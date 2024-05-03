LIBRARY()

SRCS(
    shard_reader.cpp
)

PEERDIR(
    ydb/core/formats/arrow/protos
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/kqp/compute_actor
)

END()
