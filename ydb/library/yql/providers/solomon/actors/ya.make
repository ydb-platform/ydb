LIBRARY()

SRCS(
    dq_solomon_actors_util.cpp
    dq_solomon_metrics_queue.cpp
    dq_solomon_read_actor.cpp
    dq_solomon_write_actor.cpp
)

PEERDIR(
    library/cpp/json/easy_parse
    library/cpp/monlib/encode/json
    library/cpp/protobuf/util
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/solomon/events
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/scheme
    ydb/library/yql/providers/solomon/solomon_accessor/client
    ydb/public/sdk/cpp/src/client/types/credentials
    yql/essentials/public/types
    yql/essentials/public/udf
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()

IF (OS_LINUX)
    # Solomon recipe is supported only for linux.
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
