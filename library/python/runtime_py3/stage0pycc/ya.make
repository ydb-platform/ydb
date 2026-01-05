PROGRAM()

IF (USE_PYTHON3_PREV)
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

SRCS(main.cpp)

END()
