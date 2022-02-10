DYNAMIC_LIBRARY(aio) 
 
OWNER( 
    g:contrib 
    g:cpp-contrib 
) 
 
IF (ARCH_ARMV7 OR ARCH_ARM64)
    LICENSE(
        GPL-2.0-only AND
        LGPL-2.0-or-later AND
        LGPL-2.1-only
    )
ELSE()
    LICENSE(
        LGPL-2.0-or-later AND
        LGPL-2.1-only
    )
ENDIF()
 
LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

EXPORTS_SCRIPT(libaio.exports) 
 
NO_RUNTIME() 
 
PROVIDES(libaio) 
 
DYNAMIC_LIBRARY_FROM(contrib/libs/libaio/static) 
 
END() 
