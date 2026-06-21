G_BENCHMARK()


PEERDIR(
    ydb/core/formats/arrow/reader
    ydb/core/formats/arrow
    ydb/library/formats/arrow
    contrib/libs/apache/arrow
    contrib/libs/apache/arrow_next
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    main.cpp
)

END()
