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

    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    yql/essentials/parser/pg_wrapper
    yt/yql/providers/yt/gateway/file
    yql/essentials/sql/pg

    ydb/tests/tools/kqprun/runlib
    ydb/tests/tools/kqprun/src
)

PEERDIR(
    yql/essentials/udfs/common/compress_base
    yql/essentials/udfs/common/datetime2
    yql/essentials/udfs/common/digest
    yql/essentials/udfs/common/re2
    yql/essentials/udfs/common/string
    yql/essentials/udfs/common/yson2
    yql/essentials/sql/v1
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    recipe
)

RECURSE_FOR_TESTS(
    tests
)
