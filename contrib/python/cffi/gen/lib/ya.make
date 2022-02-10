PY23_LIBRARY()

LICENSE(MIT)

OWNER(orivej)

PEERDIR(
    contrib/python/cffi
)

SRCDIR(
    contrib/python/cffi/gen
)

PY_SRCS(
    MAIN main.py
)

NO_LINT() 
 
END()
