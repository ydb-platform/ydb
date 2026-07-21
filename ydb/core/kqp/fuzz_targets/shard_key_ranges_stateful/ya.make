FUZZ()

SIZE(MEDIUM)

SRCS(
    main.cpp
    ../../executer_actor/shard_key_ranges.cpp
    ../../../tx/datashard/range_ops.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/protos
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
