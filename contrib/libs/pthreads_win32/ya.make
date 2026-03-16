LIBRARY()

LICENSE(
    LGPL-1.0-or-later AND
    LGPL-2.0-only AND
    LGPL-2.0-or-later AND
    LGPL-2.1-only AND
    LGPL-2.1-or-later AND
    LicenseRef-scancode-other-permissive
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.9.1.0)

NO_UTIL()

NO_COMPILER_WARNINGS()

NO_RUNTIME()

ADDINCL(
    GLOBAL contrib/libs/pthreads_win32/include
)

CFLAGS(
    -DPTW32_STATIC_LIB
    -DPTW32_YANDEX
    -DPTW32_BUILD_INLINED
    -DHAVE_PTW32_CONFIG_H
    -D__CLEANUP_C
)

SRCS(
    pthread.c
)

END()
