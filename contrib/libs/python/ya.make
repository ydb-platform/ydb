PY23_LIBRARY()

VERSION(Service-proxy-version)

LICENSE(Service-Py23-Proxy)

WITHOUT_LICENSE_TEXTS()

NO_PYTHON_INCLUDES()

IF (USE_ARCADIA_PYTHON)
    IF (USE_PYTHON3_PREV)
        ADDINCL(
            GLOBAL contrib/libs/python/Include_prev
        )
    ELSE()
        ADDINCL(
            GLOBAL contrib/libs/python/Include
        )
    ENDIF()

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
            library/python/runtime_py3
        )

        IF (USE_PYTHON3_PREV)
            PEERDIR(
                contrib/tools/python3_prev/lib2
                contrib/tools/python3_prev
            )

            CFLAGS(
                GLOBAL -DUSE_PYTHON3_PREV
            )
        ELSE()
            PEERDIR(
                contrib/tools/python3/lib2
                contrib/tools/python3
            )
        ENDIF()
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
