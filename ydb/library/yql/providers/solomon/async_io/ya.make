LIBRARY()

SRCS(
    dq_solomon_read_actor.cpp
    dq_solomon_write_actor.cpp
    metrics_encoder.cpp
)

PEERDIR(
    library/cpp/json/easy_parse
    library/cpp/monlib/encode/json
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/providers/solomon/proto
    ydb/library/yql/providers/solomon/scheme
    ydb/library/yql/public/types
    ydb/library/yql/public/udf
    ydb/library/yql/utils/log
)

YQL_LAST_ABI_VERSION()

END()

IF (OS_LINUX)
    # Solomon recipe is supported only for linux.
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
