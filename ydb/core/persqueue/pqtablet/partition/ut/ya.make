GTEST()


SRCS(
    consumer_offset_tracker_ut.cpp
    message_id_deduplicator_ut.cpp
)

PEERDIR(
    ydb/core/persqueue/pqtablet/partition
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
)

END()
