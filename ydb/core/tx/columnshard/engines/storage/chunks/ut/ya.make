UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/chunks)

SIZE(SMALL)

PEERDIR(
    ydb/core/formats/arrow/accessor/plain
    ydb/core/formats/arrow/serializer
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_slicing.cpp
)

END()
