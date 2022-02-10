PY23_TEST() 
 
OWNER(g:python-contrib) 
 
PEERDIR( 
    contrib/python/xmltodict 
) 
 
TEST_SRCS( 
    test_dicttoxml.py 
    test_xmltodict.py 
) 
 
NO_LINT() 
 
END() 
