UNITTEST_FOR(ydb/core/tx/columnshard/engines/scheme/indexes/abstract)

FORK_SUBTESTS()

SPLIT_FACTOR(60)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/test_helper
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/base
    ydb/core/tablet
    ydb/core/tablet_flat
    ydb/library/actors/testlib
    ydb/core/testlib

    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_program.cpp
)

END()
