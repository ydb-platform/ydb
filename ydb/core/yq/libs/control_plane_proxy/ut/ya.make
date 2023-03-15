UNITTEST_FOR(ydb/core/yq/libs/control_plane_proxy)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/testlib/default
    ydb/core/yq/libs/actors/logging
    ydb/core/yq/libs/control_plane_storage
    ydb/core/yq/libs/test_connection
    ydb/core/yq/libs/quota_manager/ut_helpers
    ydb/core/yq/libs/rate_limiter/control_plane_service
    ydb/library/folder_service
    ydb/library/folder_service/mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    control_plane_proxy_ut.cpp
)

END()
