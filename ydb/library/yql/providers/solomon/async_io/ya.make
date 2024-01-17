LIBRARY()

SRCS(
    dq_solomon_write_actor.cpp
    metrics_encoder.cpp
)

PEERDIR(
    library/cpp/json/easy_parse
    library/cpp/monlib/encode/json
    ydb/library/yql/minikql/computation
    ydb/library/yql/providers/common/token_accessor/client
    ydb/library/yql/public/types
    ydb/library/yql/public/udf
    ydb/library/yql/utils/log
    ydb/library/yql/dq/actors/compute
    ydb/library/yql/providers/solomon/proto
)

YQL_LAST_ABI_VERSION()

END()
