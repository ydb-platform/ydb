PY23_LIBRARY()

LICENSE(Service-Py23-Proxy)

WITHOUT_LICENSE_TEXTS()

NO_PYTHON_INCLUDES()

IF (USE_ARCADIA_PYTHON)
    ADDINCL(
        GLOBAL contrib/libs/python/Include
    )

    PEERDIR(
        library/python/symbols/module
        library/python/symbols/libc
        library/python/symbols/python
    )
    IF (NOT OS_WINDOWS AND NOT OPENSOURCE)
        PEERDIR(
            library/python/symbols/uuid
        )
    ENDIF()
    IF (MODULE_TAG == "PY2")
        CFLAGS(
            GLOBAL -DUSE_PYTHON2
        )
        PEERDIR(
            contrib/tools/python/lib
            library/python/runtime
        )
    ELSE()
        CFLAGS(
            GLOBAL -DUSE_PYTHON3
        )
        PEERDIR(
            contrib/tools/python3/lib
            contrib/tools/python3/src
            library/python/runtime_py3
        )
    ENDIF()
ELSE()
    IF (USE_SYSTEM_PYTHON)
        PEERDIR(
            build/platform/python
        )
    ELSE()
        CFLAGS(GLOBAL $PYTHON_INCLUDE)
    ENDIF()
ENDIF()

END()
