UNITTEST_FOR(ydb/core/formats/arrow/accessor/sub_columns)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/sub_columns
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/core/formats/arrow
)

SRCS(
    ut_sub_columns.cpp
    ut_native_scalars.cpp
    ut_dictionary.cpp
)

YQL_LAST_ABI_VERSION()

END()
