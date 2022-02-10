PY23_LIBRARY() 
 
OWNER(dankolesnikov) 
 
PY_SRCS( 
    __init__.py 
    constants.py 
    lockfile.py 
    package_json.py 
    package_manager.py 
) 
 
PEERDIR( 
    contrib/python/six 
) 
 
END() 
 
RECURSE_FOR_TESTS( 
    tests 
) 
