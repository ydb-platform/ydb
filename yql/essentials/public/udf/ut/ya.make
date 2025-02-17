UNITTEST_FOR(yql/essentials/public/udf)

SRCS(
    udf_counter_ut.cpp
    udf_value_ut.cpp
    udf_data_type_ut.cpp
    udf_value_builder_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    yql/essentials/utils
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
