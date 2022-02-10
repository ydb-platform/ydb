UNITTEST_FOR(ydb/library/yql/sql/v1) 

OWNER(g:yql)

SRCS(
    sql_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql 
)

TIMEOUT(300) 

SIZE(MEDIUM) 
 
END()
