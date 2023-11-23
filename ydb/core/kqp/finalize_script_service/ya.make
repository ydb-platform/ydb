LIBRARY()

SRCS(
    kqp_finalize_script_actor.cpp
    kqp_finalize_script_service.cpp
)

PEERDIR(
    ydb/core/kqp/proxy_service
)

YQL_LAST_ABI_VERSION()

END()
