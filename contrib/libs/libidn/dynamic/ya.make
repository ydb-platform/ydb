OWNER( 
    g:contrib
    g:cpp-contrib
) 

DYNAMIC_LIBRARY(idn)

VERSION(1.9)

LICENSE(
    Custom-Punycode
    Ietf
    LGPL-2.0-or-later
    LGPL-2.1-only
    LGPL-2.1-or-later
)

LICENSE_TEXTS(../.yandex_meta/licenses.list.txt)

VERSION(1.9)

PROVIDES(libidn)

NO_RUNTIME()

EXPORTS_SCRIPT(libidn.exports)

DYNAMIC_LIBRARY_FROM(contrib/libs/libidn/static)

END()
