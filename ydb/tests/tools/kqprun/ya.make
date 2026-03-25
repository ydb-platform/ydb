PROGRAM(kqprun)

IF (PROFILE_MEMORY_ALLOCATIONS)
    MESSAGE("Enabled profile memory allocations")
    ALLOCATOR(LF_DBG)
ENDIF()

SRCS(
    kqprun.cpp
)

PEERDIR(
    library/cpp/getopt

    ydb/core/protos
    ydb/library/testlib/common
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/tests/tools/kqprun/runlib
    ydb/tests/tools/kqprun/src

    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg

    yt/yql/providers/yt/gateway/file
)

PEERDIR(
    yql/essentials/udfs/common/compress_base
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/yson2
    yql/essentials/udfs/common/json2
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    recipe
)

RECURSE_FOR_TESTS(
    tests
)
