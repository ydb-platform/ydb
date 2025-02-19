LIBRARY()

SRCS(
    common.cpp
    fq_runner.cpp
    fq_setup.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/testing/unittest
    util
    ydb/core/fq/libs/config/protos
    ydb/core/fq/libs/control_plane_proxy/events
    ydb/core/fq/libs/init
    ydb/core/fq/libs/mock
    ydb/core/testlib
    ydb/library/folder_service/mock
    ydb/library/grpc/server/actors
    ydb/library/security
    ydb/library/yql/providers/pq/provider
    ydb/tests/tools/kqprun/runlib
)

YQL_LAST_ABI_VERSION()

END()
