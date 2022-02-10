UNITTEST_FOR(ydb/library/yql/public/udf) 

OWNER(g:yql)

SRCS(
    udf_counter_ut.cpp
    udf_value_ut.cpp
    udf_value_builder_ut.cpp
)

YQL_LAST_ABI_VERSION() 

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy 
)

END()
