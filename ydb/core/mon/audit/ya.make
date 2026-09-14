RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    audit_denylist.cpp
    audit.cpp
    url_matcher.cpp
)

PEERDIR(
    library/cpp/cgiparam
    library/cpp/json
    library/cpp/protobuf/json
    ydb/library/actors/http
    ydb/core/audit
)

END()
