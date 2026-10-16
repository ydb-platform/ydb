UNITTEST_FOR(ydb/core/tx/scheme_board)

REQUIREMENTS(cpu:1)
PEERDIR(
    library/cpp/testing/unittest
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

SRCS(
    double_indexed_ut.cpp
)

END()
