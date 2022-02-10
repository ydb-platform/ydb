OWNER(g:python-contrib)
 
PY23_TEST()
 
NO_LINT()

TEST_SRCS( 
    test_raises.py 
    test_string_description.py 
) 
 
PEERDIR( 
    contrib/python/PyHamcrest 
) 
 
END() 
