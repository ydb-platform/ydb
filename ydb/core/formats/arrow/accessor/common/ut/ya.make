UNITTEST_FOR(ydb/core/formats/arrow/accessor/common)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/common
    yql/essentials/types/binary_json
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_json_value_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
