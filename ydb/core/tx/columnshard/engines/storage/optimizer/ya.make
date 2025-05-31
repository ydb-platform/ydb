LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
    ydb/core/tx/columnshard/engines/storage/optimizer/lcbuckets
    ydb/core/tx/columnshard/engines/storage/optimizer/leveled
)

END()

RECURSE_FOR_TESTS(
    ut
)
