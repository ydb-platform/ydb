RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    audit.cpp
    url_matcher.cpp
)

PEERDIR(
    library/cpp/cgiparam
    ydb/library/actors/http
    ydb/core/audit
)

END()
