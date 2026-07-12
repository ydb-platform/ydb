UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/indexes/helper)

SRCS(
    case_helper_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    ydb/core/kqp/ut/common
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
