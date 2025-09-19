PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

FORK_TEST_FILES()

TEST_SRCS(
    base.py
    basic_reading.py
    data_paging.py
    listing_paging.py
    settings_validation.py
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/library/yql/tools/solomon_emulator/client
    ydb/tests/library
    ydb/tests/library/test_meta
)

END()
