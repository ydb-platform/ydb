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

RESOURCE(
    actors_core_ut_fat actors_core_ut_fat
)

RESOURCE(
    memory_benchmark memory_benchmark
)

RESOURCE(
    background_load background_load
)

RESOURCE(- ydb_bench/build_type=${BUILD_TYPE})

PEERDIR(
    library/python/resource
    library/python/svn_version
    ydb/tools/ydb_bench/lib
)

END()

RECURSE_FOR_TESTS(
    tests
)
