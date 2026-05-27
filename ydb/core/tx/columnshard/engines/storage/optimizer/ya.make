LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets
    ydb/core/tx/columnshard/engines/storage/optimizer/tiling
    ydb/core/tx/columnshard/engines/storage/optimizer/tiling/tiling_pp
)

END()

RECURSE_FOR_TESTS(
    ut
)
