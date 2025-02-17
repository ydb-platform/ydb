UNITTEST_FOR(yql/essentials/core/cbo)

SRCS(
    cbo_optimizer_ut.cpp
)

PEERDIR(
    yql/essentials/core/cbo
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf/service/stub
)

SIZE(SMALL)

END()
