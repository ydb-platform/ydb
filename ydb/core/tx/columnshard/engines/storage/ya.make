LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/storage/optimizer  # gn: plugin
    ydb/core/tx/columnshard/engines/storage/actualizer
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/storage/indexes  # gn: plugin
    ydb/core/tx/columnshard/engines/storage/granule
    ydb/core/formats/arrow
)

END()

RECURSE_FOR_TESTS(
    indexes
    optimizer
)
