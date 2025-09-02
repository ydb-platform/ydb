IF (NOT OS_WINDOWS)

UNITTEST_FOR(ydb/library/yql/providers/s3/path_generator)

SRCS(
    yql_generate_partitioning_rules_ut.cpp
    yql_parse_partitioning_rules_ut.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

