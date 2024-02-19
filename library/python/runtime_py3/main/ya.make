LIBRARY()

PEERDIR(
    contrib/tools/python3
    library/cpp/resource
)

ADDINCL(
    contrib/tools/python3/Include
)

CFLAGS(
    -DPy_BUILD_CORE
)

SRCS(
    main.c
    get_py_main.cpp
    venv.cpp
)

END()
