PROGRAM()

LICENSE(GPL-3.0-or-later)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

NO_RUNTIME()

NO_COMPILER_WARNINGS()

IF (MUSL)
    CFLAGS(
        -DO_BINARY=0
    )
ENDIF()

SRCS(
    src/builtin.c
    src/debug.c
    src/eval.c
    src/format.c
    src/freeze.c
    src/input.c
    src/m4.c
    src/macro.c
    src/output.c
    src/path.c
    src/symtab.c
    src/cpp.cpp
)

PEERDIR(
    contrib/tools/bison/gnulib
)

END()
