LIBRARY()

PEERDIR(
    library/cpp/resource
)

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

CFLAGS(
    -DPy_BUILD_CORE
)

SRCS(
    main.c
    get_py_main.cpp
    venv.cpp
)

END()
