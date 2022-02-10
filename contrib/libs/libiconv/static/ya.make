OWNER(
    g:contrib
    g:cpp-contrib
)

LIBRARY()

VERSION(1.13)

LICENSE(
    LGPL-2.0-only AND 
    LGPL-2.0-or-later
)
 
LICENSE_TEXTS(../.yandex_meta/licenses.list.txt)

PROVIDES(libiconv)

NO_RUNTIME()

NO_COMPILER_WARNINGS()

IF (ARCADIA_ICONV_NOCJK)
    CFLAGS( 
        -DARCADIA_ICONV_NOCJK 
    ) 
ENDIF()

SRCDIR(contrib/libs/libiconv)
 
SRCS(
    iconv.c
)

END()
