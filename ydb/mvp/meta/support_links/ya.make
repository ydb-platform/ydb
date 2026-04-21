LIBRARY()

SRCS(
    response.cpp
    source.cpp
    support_links_resolver.cpp
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/meta/protos
    library/cpp/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
