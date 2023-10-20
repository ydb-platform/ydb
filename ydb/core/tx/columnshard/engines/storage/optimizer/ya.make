LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/intervals
    ydb/core/tx/columnshard/engines/storage/optimizer/levels
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
)

END()

RECURSE_FOR_TESTS(
    ut
)
