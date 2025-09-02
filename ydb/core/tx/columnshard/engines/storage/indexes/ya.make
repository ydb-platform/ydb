LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/indexes/portions
    ydb/core/tx/columnshard/engines/storage/indexes/bloom
    ydb/core/tx/columnshard/engines/storage/indexes/bits_storage
    ydb/core/tx/columnshard/engines/storage/indexes/skip_index
    ydb/core/tx/columnshard/engines/storage/indexes/categories_bloom
    ydb/core/tx/columnshard/engines/storage/indexes/bloom_ngramm
    ydb/core/tx/columnshard/engines/storage/indexes/max
    ydb/core/tx/columnshard/engines/storage/indexes/count_min_sketch
)

END()
