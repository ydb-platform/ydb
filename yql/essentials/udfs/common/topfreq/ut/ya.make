UNITTEST_FOR(yql/essentials/udfs/common/topfreq/static)

SRCS(
    ../topfreq_udf_ut.cpp
)

PEERDIR(
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
