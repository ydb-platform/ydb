UNITTEST_FOR(ydb/core/yq/libs/control_plane_proxy)

OWNER(g:yq)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
