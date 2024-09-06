IF (NOT OS_WINDOWS)

UNITTEST_FOR(ydb/library/yql/providers/s3/actors)

SRCS(
    yql_arrow_push_down_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

