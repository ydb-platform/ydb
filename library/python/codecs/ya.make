PY23_LIBRARY()

PEERDIR(
    library/cpp/blockcodecs
    contrib/python/six
)

PY_SRCS(
    __init__.py
)

BUILDWITH_CYTHON_CPP(__codecs.pyx)

PY_REGISTER(__codecs)

END()
