LIBRARY()

SRCS(
    audit_log_item_builder.h
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

END()

RECURSE(
    audit_config
)
