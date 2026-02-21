GTEST()

SRCS(
    udf_debug_checks_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
