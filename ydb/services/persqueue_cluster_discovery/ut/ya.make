UNITTEST_FOR(ydb/services/persqueue_cluster_discovery)

FORK_SUBTESTS()

IF (WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat)
    REQUIREMENTS(ram:32)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    cluster_discovery_service_ut.cpp
)

PEERDIR(
    ydb/library/actors/http
    ydb/core/testlib/default
    ydb/public/api/grpc
    ydb/services/persqueue_cluster_discovery
)

YQL_LAST_ABI_VERSION()

END()
