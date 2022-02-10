PY23_LIBRARY()

OWNER(pg)

PEERDIR(
    library/cpp/svnversion
    contrib/python/future
)

PY_SRCS(
    __init__.py 
    __svn_version.pyx 
)

END()
