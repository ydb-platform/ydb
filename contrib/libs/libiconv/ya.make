OWNER( 
    g:contrib
    g:cpp-contrib
) 

LIBRARY()

VERSION(1.13)

LICENSE(Service-Dll-Harness)

WITHOUT_LICENSE_TEXTS()

NO_RUNTIME()

IF (USE_DYNAMIC_ICONV)
    PEERDIR(
        contrib/libs/libiconv/dynamic
    )
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
