PROGRAM(py3cc)

PYTHON3_ADDINCL()

IF (USE_PYTHON3_PREV)
    PEERDIR(
        contrib/tools/python3_prev
    )
ELSE()
    PEERDIR(
        contrib/tools/python3
    )
ENDIF()

SRCDIR(tools/py3cc)

SRCS(main.cpp)

END()
