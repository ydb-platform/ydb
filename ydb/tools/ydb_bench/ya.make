PY3_PROGRAM(ydb_bench)

PY_SRCS(
    __main__.py
)

BUNDLE(
    ydb/library/actors/core/ut_fat/bundle NAME actors_core_ut_fat
)

BUNDLE(
    ydb/tools/ydb_bench/memory NAME memory_benchmark
)

BUNDLE(
    ydb/tools/ydb_bench/background NAME background_load
)

BUNDLE(
    ydb/tools/ydb_bench/process_guard NAME process_guard_binary
)

IF(BUILD_TYPE == PROFILE)
    BUNDLE(
        ydb/apps/ydbd NAME ydbd
    )

    BUNDLE(
        ydb/apps/ydb NAME ydb_cli
    )
ELSE()
    BUNDLE(
        ydb/apps/ydbd NAME ydbd.unstripped
    )

    RUN_PROGRAM(
        contrib/libs/llvm18/tools/llvm-objcopy --strip-all ydbd.unstripped ydbd
        IN ydbd.unstripped
        OUT ydbd
    )

    BUNDLE(
        ydb/apps/ydb NAME ydb_cli.unstripped
    )

    RUN_PROGRAM(
        contrib/libs/llvm18/tools/llvm-objcopy --strip-all ydb_cli.unstripped ydb_cli
        IN ydb_cli.unstripped
        OUT ydb_cli
    )
ENDIF()

RESOURCE(
    actors_core_ut_fat actors_core_ut_fat
)

RESOURCE(
    memory_benchmark memory_benchmark
)

RESOURCE(
    background_load background_load
)

RESOURCE(
    process_guard_binary process_guard
)

RESOURCE(
    ydbd ydbd
)

RESOURCE(
    ydb_cli ydb_cli
)

RESOURCE(- ydb_bench/build_type=${BUILD_TYPE})

PEERDIR(
    library/python/resource
    library/python/svn_version
    ydb/tools/ydb_bench/lib
)

END()

RECURSE(
    process_guard
)

RECURSE_FOR_TESTS(
    tests
)
