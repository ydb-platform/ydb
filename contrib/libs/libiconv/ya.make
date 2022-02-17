OWNER(
    g:contrib
    g:cpp-contrib
)

LIBRARY()

VERSION(1.13)

LICENSE(Service-Dll-Harness)

WITHOUT_LICENSE_TEXTS()

NO_RUNTIME()

OPENSOURCE_EXPORT_REPLACEMENT(
    CMAKE Iconv
    CMAKE_TARGET Iconv::Iconv
    CONAN libiconv/1.15
)

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
