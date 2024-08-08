UNITTEST_FOR(ydb/core/fq/libs/control_plane_proxy)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/base
    ydb/core/fq/libs/actors/logging
    ydb/core/fq/libs/control_plane_storage
    ydb/core/fq/libs/test_connection
    ydb/core/fq/libs/quota_manager/ut_helpers
    ydb/core/fq/libs/rate_limiter/control_plane_service
    ydb/core/testlib/default
    ydb/library/folder_service
    ydb/library/folder_service/mock
)

YQL_LAST_ABI_VERSION()

SRCS(
    control_plane_proxy_ut.cpp
)

END()
