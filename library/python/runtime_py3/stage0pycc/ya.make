PROGRAM()

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

SRCS(main.cpp)

END()
