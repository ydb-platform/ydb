UNITTEST_FOR(ydb/core/formats/arrow/accessor/dictionary)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/dictionary
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_dictionary.cpp
)

END()
