UNITTEST_FOR(yql/essentials/minikql/udf_value_test_support)

SRCS(
    udf_value_comparator_utils_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
