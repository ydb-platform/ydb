LIBRARY()

LICENSE(MIT)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(v1.10-4-g57678b1-25.01.2016)

NO_COMPILER_WARNINGS()

NO_UTIL()

ADDINCL(
    contrib/libs/lbfgs
    contrib/libs/lbfgs/lib
    GLOBAL contrib/libs/lbfgs/include
)

IF (NOT OS_WINDOWS)
    CFLAGS(
        -DHAVE_CONFIG_H
        -ffast-math
    )

    IF (SANITIZER_TYPE == "memory")
        CFLAGS(-fno-finite-math-only)
    ENDIF()
ENDIF()

SRCS(
    lib/lbfgs.c
)

END()
