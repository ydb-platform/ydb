LIBRARY()

SRCS(
    partition_scale_request.cpp
    partition_scale_manager.cpp
    read_balancer__balancing_app.cpp
    read_balancer__balancing.cpp
    read_balancer_app.cpp
    read_balancer.cpp
)

GENERATE_ENUM_SERIALIZATION(read_balancer__balancing.h)

PEERDIR(
    ydb/library/actors/core
    library/cpp/html/pcdata
    library/cpp/json
    ydb/core/backup/impl
    ydb/core/base
    ydb/core/engine/minikql
    ydb/core/keyvalue
    ydb/core/kqp/common
    ydb/core/metering
    ydb/core/persqueue/codecs
    ydb/core/persqueue/config
    ydb/core/persqueue/events
    ydb/core/persqueue/partition_key_range
    ydb/core/persqueue/writer
    ydb/core/protos
    ydb/library/logger
    ydb/library/persqueue/counter_time_keeper
    ydb/library/persqueue/topic_parser
    ydb/library/protobuf_printer
    ydb/public/lib/base
    ydb/public/sdk/cpp/src/client/persqueue_public
    #ydb/library/dbgtrace
)

END()

RECURSE_FOR_TESTS(
#    ut
#    dread_cache_service/ut
#    ut/slow
#    ut/ut_with_sdk
)
