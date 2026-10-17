UNITTEST_FOR(ydb/core/fq/libs/compute/ydb/control_plane)

SIZE(SMALL)

PEERDIR(
    ydb/core/fq/libs/control_plane_storage
    ydb/core/testlib/default
    ydb/library/security
)

SRCS(
    compute_database_control_plane_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
