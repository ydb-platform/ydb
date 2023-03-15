LIBRARY()

SRCS(
    audit_log_impl.cpp
    audit_log_impl.h
    audit_log.cpp
    audit_log.h
)

PEERDIR(
    library/cpp/actors/core
    library/cpp/json
    library/cpp/logger
    ydb/core/base
)

RESOURCE(
    ydb/core/kqp/kqp_default_settings.txt kqp_default_settings.txt
)

END()
