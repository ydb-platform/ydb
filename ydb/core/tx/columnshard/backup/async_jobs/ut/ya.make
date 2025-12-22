UNITTEST_FOR(ydb/core/tx/columnshard/backup/async_jobs)

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    ydb/apps/ydbd/export
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/columnshard/hooks/abstract
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/library/aclib/protos
    ydb/public/lib/yson_value
    ydb/services/metadata
    ydb/library/testlib/s3_recipe_helper
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

SRCS(
    ut_import_downloader.cpp
)

END()
