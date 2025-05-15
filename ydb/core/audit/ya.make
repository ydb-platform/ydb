LIBRARY()

SRCS(
    audit_log_item_builder.h
    audit_log.h
    audit_log_service.h
    audit_log_impl.cpp
    audit_log_helpers.cpp
)

PEERDIR(
    ydb/library/actors/core
    library/cpp/json
    library/cpp/logger
    ydb/core/base
    ydb/public/api/client/yc_public/events
)

END()
