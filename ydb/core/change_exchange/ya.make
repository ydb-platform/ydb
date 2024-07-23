LIBRARY()

SRCS(
    change_exchange.cpp
    change_record.cpp
    change_sender_monitoring.cpp
    change_sender_partitioner.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
    ydb/library/actors/core
    ydb/library/yverify_stream
    library/cpp/monlib/service/pages
)

YQL_LAST_ABI_VERSION()

END()
