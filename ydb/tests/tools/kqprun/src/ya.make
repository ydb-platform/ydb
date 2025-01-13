LIBRARY()

SRCS(
    actors.cpp
    common.cpp
    kqp_runner.cpp
    ydb_setup.cpp
)

PEERDIR(
    ydb/core/testlib

    ydb/tests/tools/kqprun/src/proto
)

GENERATE_ENUM_SERIALIZATION(common.h)

YQL_LAST_ABI_VERSION()

END()
