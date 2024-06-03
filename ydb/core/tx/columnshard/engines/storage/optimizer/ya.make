LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/lbuckets
    ydb/core/tx/columnshard/engines/storage/optimizer/sbuckets
)

END()

RECURSE_FOR_TESTS(
    ut
)
