UNITTEST_FOR(ydb/core/fq/libs/result_formatter)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    result_formatter_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/services/ydb

    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/library/yql/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

END()
