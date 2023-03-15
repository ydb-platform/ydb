LIBRARY()

SRCS(
    yq_audit_service.cpp
)

PEERDIR(
    library/cpp/actors/core
)

END()

RECURSE(
    events
)
