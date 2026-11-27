UNITTEST_FOR(ydb/core/persqueue/public/partition_key_range)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

SRCS(
    partition_key_range_sequence_ut.cpp
    partition_key_range_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
