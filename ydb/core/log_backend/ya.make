LIBRARY()

SRCS(
    log_backend.cpp
    log_backend.h
    log_backend_build.cpp
    log_backend_build.h
)

PEERDIR(
    ydb/core/base
    ydb/core/driver_lib/cli_config_base

    library/cpp/messagebus/monitoring
    library/cpp/unified_agent_client
)

YQL_LAST_ABI_VERSION()

END()
