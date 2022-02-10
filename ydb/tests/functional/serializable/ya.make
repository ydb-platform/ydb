OWNER(g:kikimr)

PY3TEST() 

PEERDIR( 
    ydb/tests/tools/ydb_serializable/lib 
    ydb/public/sdk/python/ydb 
) 

TEST_SRCS(test.py) 

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc) 

SIZE(MEDIUM) 
END() 
