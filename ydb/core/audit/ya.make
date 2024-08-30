LIBRARY()

SRCS(
    audit_log.h
    audit_log_service.h
    audit_log_impl.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/json
    library/cpp/logger
    ydb/core/base
)

RESOURCE(
    ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt
)

END()
