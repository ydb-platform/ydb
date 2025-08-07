RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    auditable_actions.cpp
    audit.cpp
    url_matcher.cpp
)

PEERDIR(
    library/cpp/cgiparam
    ydb/library/actors/http
    ydb/core/audit
)

END()
