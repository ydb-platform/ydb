OWNER(
    g:contrib
    g:cpp-contrib
)

LIBRARY() 
 
LICENSE(Service-Dll-Harness)

WITHOUT_LICENSE_TEXTS()
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(1.9) 
 
NO_RUNTIME() 
 
NO_COMPILER_WARNINGS()

IF (USE_DYNAMIC_IDN)
    PEERDIR( 
        contrib/libs/libidn/dynamic 
    ) 
ELSE() 
    PEERDIR(
        contrib/libs/libidn/static
    )
ENDIF() 
 
END() 
 
RECURSE(
    dynamic
    static
)
