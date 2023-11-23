PY23_LIBRARY()

PY_SRCS(
    NAMESPACE yt.type_info

    __init__.py
    typing.py
    type_base.py
)

PEERDIR(
    contrib/python/six
)

END()

RECURSE(
    test
)
