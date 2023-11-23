LIBRARY()

LICENSE("(BSD-3-Clause OR GPL-2.0-only)")

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2.25)

ORIGINAL_SOURCE(https://mirrors.edge.kernel.org/pub/linux/libs/security/linux-privs/libcap2/)

ADDINCL(
    GLOBAL contrib/libs/libcap/include
)

NO_COMPILER_WARNINGS()

NO_UTIL()

SRCS(
    cap_alloc.c
    cap_extint.c
    cap_file.c
    cap_flag.c
    cap_proc.c
    cap_text.c
)

END()
