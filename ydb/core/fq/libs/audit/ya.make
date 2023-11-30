LIBRARY()

SRCS(
    yq_audit_service.cpp
)

PEERDIR(
    ydb/library/actors/core
)

END()

RECURSE(
    events
)
