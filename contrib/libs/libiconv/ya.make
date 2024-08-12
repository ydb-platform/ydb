LIBRARY()

VERSION(1.13)

LICENSE(Service-Dll-Harness)

WITHOUT_LICENSE_TEXTS()

NO_RUNTIME()

DEFAULT(USE_ICONV ${_USE_ICONV})

IF (EXPORT_CMAKE)
    IF (OS_WINDOWS)
        OPENSOURCE_EXPORT_REPLACEMENT(
            CMAKE
            Iconv
            CMAKE_TARGET
            Iconv::Iconv
            CONAN
            libiconv/1.15 "&& conan-requires" libiconv/1.15
            CONAN_OPTIONS
            libiconv:shared=True
        )
    ELSE()
        # Opensource code is compatible with libc provided iconv API on major linux distributions and macos.
        #  * We prefere to avoid vendoring LGPL libraries in our opensouce project
        #  * Major distributions do not provide GNU libiconv as separate package
        #  * Shared lib dependencies from conan overcomplicate final artefacts distribution
        DISABLE(OPENSOURCE_EXPORT)
    ENDIF()
ELSEIF (USE_ICONV == "dynamic")
    PEERDIR(
        contrib/libs/libiconv/dynamic
    )
ELSEIF (USE_ICONV == "local")
    GLOBAL_CFLAGS(${USE_LOCAL_ICONV_CFLAGS})

    IF (OS_DARWIN)
        LDFLAGS(-liconv)
    ENDIF()

    # Opensource code is compatible with libc provided iconv API on major linux distributions and macos.
ELSE()
    PEERDIR(
        contrib/libs/libiconv/static
    )
ENDIF()

END()

RECURSE(
    dynamic
    static
)
