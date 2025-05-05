GTEST()
INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    bulk_upsert.cpp
    main.cpp
)

END()
