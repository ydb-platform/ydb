PY_ANY_MODULE(_cyson)

IF (PYTHON_CONFIG MATCHES "python3" OR USE_SYSTEM_PYTHON MATCHES "3.")
    PYTHON3_MODULE()
ELSE()
    PYTHON2_MODULE()
ENDIF()

NO_WSHADOW()

PEERDIR(
    library/c/cyson
)

SRCS(
    library/python/cyson/cyson/_cyson.pyx
    library/python/cyson/cyson/helpers.cpp
    library/python/cyson/cyson/unsigned_long.cpp
)

END()
