UNITTEST_FOR(ydb/library/plan2svg)

SIZE(MEDIUM)

SRCS(
    plan2svg_ut.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/testing/common
    library/cpp/xml/document
)

DATA(
    arcadia/ydb/library/plan2svg/ut/data
)

END()
