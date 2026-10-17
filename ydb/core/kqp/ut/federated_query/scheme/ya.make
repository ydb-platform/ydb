UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(10)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:4)
ELSE()
    REQUIREMENTS(cpu:2)
ENDIF()

SRCS(
    external_data_source_ut.cpp
    external_table_ut.cpp
    streaming_query_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/yql/providers/pq/proto
    ydb/services/workload_manager/ut/common
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
