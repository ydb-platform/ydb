UNITTEST_FOR(ydb/library/plan2svg)

SIZE(MEDIUM)

SRCS(
    plan2svg_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/common
)

DATA(
    arcadia/ydb/library/plan2svg/ut/data
)

END()
