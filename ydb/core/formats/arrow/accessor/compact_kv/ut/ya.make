UNITTEST_FOR(ydb/core/formats/arrow/accessor/compact_kv)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/compact_kv
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yql/essentials/types/binary_json
    ydb/core/formats/arrow
)

SRCS(
    ut_compact_kv.cpp
)

YQL_LAST_ABI_VERSION()

END()
