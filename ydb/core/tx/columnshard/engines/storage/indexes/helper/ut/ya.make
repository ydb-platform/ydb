UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/indexes/helper)

SRCS(
    case_helper_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
