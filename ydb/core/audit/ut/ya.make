UNITTEST()

PEERDIR(
    ydb/library/actors/testlib
    ydb/core/audit/heartbeat_actor
    ydb/core/util
)

SRCS(
    audit_log_service_ut.cpp
    audit_log_ut.cpp
)

END()
