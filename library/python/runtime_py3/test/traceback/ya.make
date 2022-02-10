PY3_PROGRAM() 
 
OWNER(
    abodrov
    borman
)
 
PEERDIR( 
    contrib/python/ipython 
) 
 
PY_SRCS( 
    MAIN
    __main__.py=main
    crash.py 
    mod/__init__.py 
) 
 
END() 
