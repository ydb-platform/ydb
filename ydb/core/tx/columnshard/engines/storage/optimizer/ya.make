LIBRARY()

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/abstract
    ydb/core/tx/columnshard/engines/storage/optimizer/intervals
    ydb/core/tx/columnshard/engines/storage/optimizer/levels
)

END()

RECURSE_FOR_TESTS(
    ut
)
