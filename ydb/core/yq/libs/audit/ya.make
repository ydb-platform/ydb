OWNER(g:yq) 

LIBRARY()

SRCS(
    yq_audit_service.cpp
)

END()

RECURSE(
    events
    mock
)
