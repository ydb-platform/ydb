# sources downloaded from: https://git.fedorahosted.org/cgit/libaio.git
LIBRARY()

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
 
OWNER(
    vskipin
    g:contrib
    g:cpp-contrib
)

NO_UTIL()

NO_RUNTIME()

PROVIDES(libaio)

SRCDIR(contrib/libs/libaio)

ADDINCL(
    contrib/libs/libaio
)

SRCS(
    io_cancel.c
    io_destroy.c
    io_getevents.c
    io_queue_init.c
    io_queue_release.c
    io_queue_run.c
    io_queue_wait.c
    io_setup.c
    io_submit.c
    raw_syscall.c
)

END()
