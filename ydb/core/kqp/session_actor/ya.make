LIBRARY()

SRCS(
    kqp_response.cpp
    kqp_session_actor.cpp
    kqp_tx.cpp
    kqp_worker_actor.cpp
    kqp_worker_common.cpp
    kqp_query_state.cpp
    kqp_temp_tables_manager.cpp
)

PEERDIR(
    ydb/core/docapi
    ydb/core/kqp/common
    ydb/library/yql/providers/common/http_gateway
    ydb/public/lib/operation_id
)

YQL_LAST_ABI_VERSION()

END()
