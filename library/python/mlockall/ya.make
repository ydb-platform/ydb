PY23_LIBRARY()

PY_SRCS(
    __init__.py
)

IF (OS_LINUX)
    PY_SRCS(
        mlockall.pyx
    )
ENDIF()

END()

