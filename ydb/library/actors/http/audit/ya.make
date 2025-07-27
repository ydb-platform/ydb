LIBRARY()

SRCS(
    audit.cpp
    auditable_actions.cpp
)

PEERDIR(
    library/cpp/cgiparam
    ydb/library/actors/http
    ydb/core/audit
)

END()
