UNITTEST_FOR(ydb/core/fq/libs/common)

FORK_SUBTESTS()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    cache_ut.cpp
    entity_id_ut.cpp
    rows_proto_splitter_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
