PY2TEST()
 
OWNER(g:passport_infra) 
 
TEST_SRCS(test.py)
 
DEPENDS(library/cpp/tvmauth/src/rw/ut_large/gen) 
 
TAG(ya:fat) 

SIZE(LARGE) 
 
END() 
 
RECURSE(
    gen
)
