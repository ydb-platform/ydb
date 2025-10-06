UNITTEST_FOR(yql/essentials/udfs/common/stat/static)

ENABLE(YQL_STYLE_CPP)

SRCS(
    ../stat_udf_ut.cpp
)

PEERDIR(
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

TIMEOUT(300)

SIZE(MEDIUM)

END()
