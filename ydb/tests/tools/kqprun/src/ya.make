LIBRARY()

SRCS(
    actors.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    ydb/core/kqp/workload_service/actors
    ydb/core/testlib

    ydb/library/aclib

    ydb/tests/tools/kqprun/runlib
    ydb/tests/tools/kqprun/src/proto

    yt/yql/providers/yt/mkql_dq
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
