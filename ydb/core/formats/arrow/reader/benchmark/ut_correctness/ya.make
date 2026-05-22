UNITTEST_FOR(ydb/core/formats/arrow/reader)


PEERDIR(
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow
    ydb/library/formats/arrow
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    library/cpp/testing/benchmark
)

SRCS(
    ut_correctness.cpp
)

END()
