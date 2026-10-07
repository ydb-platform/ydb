G_BENCHMARK()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/formats/arrow/accessor/dictionary
    ydb/core/formats/arrow/accessor/plain
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    main.cpp
)

END()
