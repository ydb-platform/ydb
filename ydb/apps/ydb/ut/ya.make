UNITTEST()

DEPENDS(
    ydb/apps/ydb
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/ydb_topic
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

END()
