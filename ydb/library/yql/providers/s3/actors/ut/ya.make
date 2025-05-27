IF (NOT OS_WINDOWS)

UNITTEST_FOR(ydb/library/yql/providers/s3/actors)

SRCS(
    yql_arrow_push_down_ut.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

