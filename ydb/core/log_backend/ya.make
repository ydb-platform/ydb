LIBRARY()

SRCS(
    log_backend.cpp
    log_backend.h
    log_backend_build.cpp
    log_backend_build.h
)

PEERDIR(
    library/cpp/unified_agent_client
    ydb/core/base
)

YQL_LAST_ABI_VERSION()

END()
