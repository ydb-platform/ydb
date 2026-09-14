UNITTEST_FOR(ydb/library/yql/providers/pq/common)

SRCS(
    pq_meta_fields_ut.cpp
    pq_shared_reading_ut.cpp
)

PEERDIR(
    ydb/library/yql/providers/pq/common
)

YQL_LAST_ABI_VERSION()

END()
