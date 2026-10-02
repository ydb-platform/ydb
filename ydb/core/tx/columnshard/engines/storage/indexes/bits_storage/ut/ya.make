UNITTEST_FOR(ydb/core/tx/columnshard/engines/storage/indexes/bits_storage)

SRCS(
    bits_storage_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

END()
