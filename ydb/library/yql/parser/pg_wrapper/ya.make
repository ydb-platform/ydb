LIBRARY()

PROVIDES(
    yql_pg_runtime
)

YQL_LAST_ABI_VERSION()

ADDINCL(
    contrib/libs/libiconv/include
    contrib/libs/lz4
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/bootstrap
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/parser
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/replication/logical
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/adt
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/misc
    ydb/library/yql/parser/pg_wrapper/postgresql/src/backend/utils/sort
    ydb/library/yql/parser/pg_wrapper/postgresql/src/common
    ydb/library/yql/parser/pg_wrapper/postgresql/src/include
    contrib/libs/postgresql/src/port
)

SRCS(
    arena_ctx.cpp
    arrow.cpp
    arrow_impl.cpp
    parser.cpp
    thread_inits.c
    comp_factory.cpp
    type_cache.cpp
    pg_aggs.cpp
    pg_kernels.0.cpp
    pg_kernels.1.cpp
    pg_kernels.2.cpp
    pg_kernels.3.cpp
    config.cpp
    cost_mocks.cpp
)

IF (ARCH_X86_64)
    CFLAGS(
        -DHAVE__GET_CPUID=1
        -DUSE_SSE42_CRC32C_WITH_RUNTIME_CHECK=1
    )
    SRCS(
        postgresql/src/port/pg_crc32c_sse42.c
        postgresql/src/port/pg_crc32c_sse42_choose.c
    )
ENDIF()

# DTCC-950
NO_COMPILER_WARNINGS()

INCLUDE(pg_sources.inc)

IF (NOT OPENSOURCE AND NOT OS_WINDOWS AND NOT SANITIZER_TYPE AND NOT BUILD_TYPE == "DEBUG")
INCLUDE(pg_bc.0.inc)
INCLUDE(pg_bc.1.inc)
INCLUDE(pg_bc.2.inc)
INCLUDE(pg_bc.3.inc)
ELSE()
CFLAGS(-DUSE_SLOW_PG_KERNELS)
ENDIF()

PEERDIR(
    library/cpp/resource
    library/cpp/yson
    ydb/library/yql/core
    ydb/library/yql/minikql/arrow
    ydb/library/yql/minikql/comp_nodes/llvm
    ydb/library/yql/parser/pg_catalog
    ydb/library/yql/providers/common/codec
    ydb/library/yql/public/issue
    ydb/library/yql/public/udf
    ydb/library/yql/utils

    contrib/libs/icu
    contrib/libs/libc_compat
    contrib/libs/libiconv
    contrib/libs/libxml
    contrib/libs/lz4
    contrib/libs/openssl
)

INCLUDE(cflags.inc)

IF (OS_LINUX OR OS_DARWIN)
    SRCS(
        ../../../../../contrib/libs/postgresql/src/backend/port/posix_sema.c
        ../../../../../contrib/libs/postgresql/src/backend/port/sysv_shmem.c
    )
ELSEIF (OS_WINDOWS)
    ADDINCL(
        contrib/libs/postgresql/src/include/port
        contrib/libs/postgresql/src/include/port/win32
        contrib/libs/postgresql/src/include/port/win32_msvc
    )
    SRCS(
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/crashdump.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/signal.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/socket.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32/timer.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32_sema.c
        ../../../../../contrib/libs/postgresql/src/backend/port/win32_shmem.c
        ../../../../../contrib/libs/postgresql/src/port/dirmod.c
        ../../../../../contrib/libs/postgresql/src/port/dlopen.c
        ../../../../../contrib/libs/postgresql/src/port/getaddrinfo.c
        ../../../../../contrib/libs/postgresql/src/port/getopt.c
        ../../../../../contrib/libs/postgresql/src/port/getrusage.c
        ../../../../../contrib/libs/postgresql/src/port/gettimeofday.c
        ../../../../../contrib/libs/postgresql/src/port/inet_aton.c
        ../../../../../contrib/libs/postgresql/src/port/kill.c
        ../../../../../contrib/libs/postgresql/src/port/open.c
        ../../../../../contrib/libs/postgresql/src/port/pread.c
        ../../../../../contrib/libs/postgresql/src/port/pwrite.c
        ../../../../../contrib/libs/postgresql/src/port/pwritev.c
        ../../../../../contrib/libs/postgresql/src/port/system.c
        ../../../../../contrib/libs/postgresql/src/port/win32env.c
        ../../../../../contrib/libs/postgresql/src/port/win32error.c
        ../../../../../contrib/libs/postgresql/src/port/win32security.c
        ../../../../../contrib/libs/postgresql/src/port/win32setlocale.c
        ../../../../../contrib/libs/postgresql/src/port/win32stat.c
    )
ENDIF()

END()

RECURSE(
    interface
)

RECURSE_FOR_TESTS(
    ut
)
