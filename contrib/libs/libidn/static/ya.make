OWNER(
    g:contrib
    g:cpp-contrib
)

LIBRARY()

LICENSE(
    Custom-Punycode AND
    Ietf AND
    LGPL-2.0-or-later AND
    LGPL-2.1-only AND
    LGPL-2.1-or-later
)

LICENSE_TEXTS(../.yandex_meta/licenses.list.txt)

VERSION(1.9)

PROVIDES(libidn)

NO_RUNTIME()

NO_COMPILER_WARNINGS()

ADDINCL(
    contrib/libs/libidn/lib
    contrib/libs/libidn/lib/gl
)

CFLAGS(
    -DHAVE_CONFIG_H
)

IF (OS_WINDOWS)
    CFLAGS(
        -DLIBIDN_EXPORTS
    )
ENDIF()

IF (OS_ANDROID)
    CFLAGS(
        -DHAVE_LOCALE_H=1
    )
ENDIF()

SRCDIR(contrib/libs/libidn)

SRCS(
    lib/idn-free.c
    lib/idna.c
    lib/nfkc.c
    lib/pr29.c
    lib/profiles.c
    lib/punycode.c
    lib/rfc3454.c
    lib/strerror-idna.c
    lib/strerror-pr29.c
    lib/strerror-punycode.c
    lib/strerror-stringprep.c
    lib/strerror-tld.c
    lib/stringprep.c
    lib/tld.c
    lib/tlds.c
    lib/toutf8.c
    lib/version.c
    lib/gl/strverscmp.c
    lib/gl/striconv.c
    lib/gl/c-strcasecmp.c
    lib/gl/c-ctype.c
)

END()
