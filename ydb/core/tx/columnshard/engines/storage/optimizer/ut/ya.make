UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/optimizer)

SIZE(SMALL)

PEERDIR(
    ydb/core/tx/columnshard/engines/storage/optimizer/tiling
)

SRCS(
    ut_tiling.cpp
)

END()
