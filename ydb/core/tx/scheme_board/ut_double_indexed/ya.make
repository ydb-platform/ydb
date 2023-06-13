UNITTEST_FOR(ydb/core/tx/scheme_board)

PEERDIR(
    library/cpp/testing/unittest
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/public/udf/service/exception_policy
)

SRCS(
    double_indexed_ut.cpp
)

END()
