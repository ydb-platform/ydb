DYNAMIC_LIBRARY(iconv)

VERSION(1.13)

LICENSE(
    LGPL-2.0-only AND
    LGPL-2.0-or-later
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

PROVIDES(libiconv)

NO_RUNTIME()

EXPORTS_SCRIPT(libiconv.exports)

DYNAMIC_LIBRARY_FROM(contrib/libs/libiconv/static)

LICENSE_RESTRICTION_EXCEPTIONS(contrib/libs/libiconv/static)

END()
