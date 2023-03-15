UNITTEST_FOR(ydb/core/yq/libs/control_plane_proxy)

PEERDIR(
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

SRCS(
    test_connection_ut.cpp
)

END()
