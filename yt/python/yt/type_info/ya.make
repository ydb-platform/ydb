PY23_LIBRARY()

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/type_info)
ELSE()
    PY_SRCS(
        NAMESPACE yt.type_info

        __init__.py
        typing.py
        type_base.py
    )

    PEERDIR(
        contrib/python/six
    )
ENDIF()

END()

RECURSE(
    test
)
