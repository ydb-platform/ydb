LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSL-1.0
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(3.0.1)

NO_UTIL()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/libs/msgpack/include
)

SRCS(
    src/objectc.c
    src/unpack.c
    src/version.c
    src/vrefbuffer.c
    src/zone.c
)

PEERDIR(
    contrib/libs/libc_compat
)

END()
