PY23_LIBRARY()

IF(OS_LINUX)
PY_SRCS(
    __init__.py
    nstools.pyx
)
ELSE()
PY_SRCS(
    nstools.py
)
ENDIF()

END()
