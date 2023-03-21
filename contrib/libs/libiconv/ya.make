LIBRARY()

VERSION(1.13)

LICENSE(Service-Dll-Harness)

WITHOUT_LICENSE_TEXTS()

NO_RUNTIME()

IF(OS_WINDOWS)
    OPENSOURCE_EXPORT_REPLACEMENT(
        CMAKE Iconv
        CMAKE_TARGET Iconv::Iconv
        CONAN libiconv/1.15
        CONAN_OPTIONS libiconv:shared=True
    )
ELSE()
    # Opensource code is compatible with libc provided iconv API on major linux distributions and macos.
    #  * We prefere to avoid vendoring LGPL libraries in our opensouce project
    #  * Major distributions do not provide GNU libiconv as separate package
    #  * Shared lib dependencies from conan overcomplicate final artefacts distribution
    DISABLE(OPENSOURCE_EXPORT)
ENDIF()

IF (NOT EXPORT_CMAKE)

IF (USE_DYNAMIC_ICONV)
    PEERDIR(
        contrib/libs/libiconv/dynamic
    )
ELSE()
    PEERDIR(
        contrib/libs/libiconv/static
    )
ENDIF()

ENDIF()

END()

RECURSE(
    dynamic
    static
)
