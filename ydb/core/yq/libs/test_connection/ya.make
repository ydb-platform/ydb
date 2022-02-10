OWNER(g:yq)

LIBRARY()

SRCS(
    test_connection.cpp
    probes.cpp
)

PEERDIR(
    library/cpp/lwtrace
    ydb/core/yq/libs/actors/logging
    ydb/core/yq/libs/config/protos
    ydb/core/yq/libs/test_connection/events
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    events
)

RECURSE_FOR_TESTS(
    ut
)
