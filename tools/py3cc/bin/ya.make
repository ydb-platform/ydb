PROGRAM(py3cc)

IF (NOT USE_ARCADIA_PYTHON)
    PYTHON3_ADDINCL()

    PEERDIR(
        contrib/tools/python3
    )
ELSEIF (USE_PYTHON3_PREV)
    PEERDIR(
        contrib/tools/python3_prev
    )
    ADDINCL(
        contrib/tools/python3_prev/Include
    )
ELSE()
    PEERDIR(
        contrib/tools/python3
    )
    ADDINCL(
        contrib/tools/python3/Include
    )
ENDIF()

SRCDIR(tools/py3cc)

SRCS(main.cpp)

END()
