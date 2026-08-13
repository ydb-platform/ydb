PY3_PROGRAM(ydb_bench)

PY_SRCS(
    __main__.py
)

BUNDLE(
    ydb/library/actors/core/ut_fat/bundle NAME actors_core_ut_fat
)

RESOURCE(
    actors_core_ut_fat actors_core_ut_fat
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
