PROGRAM(fqrun)

IF (PROFILE_MEMORY_ALLOCATIONS)
    MESSAGE("Enabled profile memory allocations")
    ALLOCATOR(LF_DBG)
    CFLAGS(-D PROFILE_MEMORY_ALLOCATIONS)
ENDIF()

SRCS(
    fqrun.cpp
)

PEERDIR(
    library/cpp/colorizer
    library/cpp/getopt
    library/cpp/lfalloc/alloc_profiler
    ydb/core/blob_depot
    ydb/library/yql/providers/pq/gateway/dummy
    ydb/tests/tools/fqrun/src
    ydb/tests/tools/kqprun/runlib
    yql/essentials/parser/pg_wrapper
    yql/essentials/sql/pg
)

PEERDIR(
    yql/essentials/udfs/common/compress_base
    yql/essentials/udfs/common/re2
)

YQL_LAST_ABI_VERSION()

END()
