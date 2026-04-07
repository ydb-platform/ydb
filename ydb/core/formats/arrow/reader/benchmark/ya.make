Y_BENCHMARK()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
SIZE(LARGE)

ALLOCATOR(LF)

PEERDIR(
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow
    ydb/library/formats/arrow
    contrib/libs/apache/arrow
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    main.cpp
)

END()
