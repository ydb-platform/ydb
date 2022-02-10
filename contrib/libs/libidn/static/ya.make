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
    contrib/libs/libidn
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
    idn-free.c
    idna.c
    nfkc.c
    pr29.c
    profiles.c
    punycode.c
    rfc3454.c
    strerror-idna.c
    strerror-pr29.c
    strerror-punycode.c
    strerror-stringprep.c
    strerror-tld.c
    stringprep.c
    tld.c
    tlds.c
    toutf8.c
    version.c
    strverscmp.c
    striconv.c
    c-strcasecmp.c
    c-ctype.c
)

END()
