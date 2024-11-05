UNITTEST_FOR(ydb/library/yql/public/udf)

SRCS(
    udf_counter_ut.cpp
    udf_value_ut.cpp
    udf_data_type_ut.cpp
    udf_value_builder_ut.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    ydb/library/yql/utils
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
)

END()
