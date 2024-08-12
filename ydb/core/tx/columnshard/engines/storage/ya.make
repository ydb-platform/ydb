LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/protos
    ydb/core/tx/columnshard/engines/storage/optimizer
    ydb/core/tx/columnshard/engines/storage/actualizer
    ydb/core/tx/columnshard/engines/storage/chunks
    ydb/core/tx/columnshard/engines/storage/indexes
    ydb/core/tx/columnshard/engines/storage/granule
    ydb/core/formats/arrow
)

END()
