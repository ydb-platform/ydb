LIBRARY()

VERSION(5.3-1.0.0.1)

BUILD_ONLY_IF(WARNING OS_LINUX)

SRCS(
    impl.cpp
    symbols.cpp
)

ADDINCL(
    GLOBAL contrib/libs/ibdrv/include
)

LICENSE(
    "((GPL-2.0-only WITH Linux-syscall-note) OR Linux-OpenIB)" AND
    "(GPL-2.0-only OR Linux-OpenIB)"
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

END()
