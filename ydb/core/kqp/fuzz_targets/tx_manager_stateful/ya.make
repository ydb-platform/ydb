FUZZ()

SIZE(LARGE)

TAG(
    ya:fat
)

SRCS(
    main.cpp
    ../../executer_actor/kqp_locks_helper.cpp
)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/topics
    ydb/core/persqueue/public
    ydb/core/tx/locks
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
