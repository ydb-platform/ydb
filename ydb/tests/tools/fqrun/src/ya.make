LIBRARY()

SRCS(
    actors.cpp
    common.cpp
    fq_runner.cpp
    fq_setup.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/testing/unittest
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/init
    ydb/core/fq/libs/mock
    ydb/core/testlib
    ydb/library/actors/core
    ydb/library/folder_service/mock
    ydb/library/grpc/server/actors
    ydb/library/security
    ydb/library/yql/providers/pq/provider
    ydb/tests/tools/kqprun/runlib
    yql/essentials/minikql
)

YQL_LAST_ABI_VERSION()

END()
