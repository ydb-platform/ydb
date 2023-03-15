UNITTEST_FOR(ydb/core/tx/schemeshard)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/tx/tx_proxy
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
)

SRCS(
    ut_ru_calculator.cpp
)

YQL_LAST_ABI_VERSION()

END()
