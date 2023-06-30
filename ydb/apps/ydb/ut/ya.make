UNITTEST()

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    main.cpp
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
