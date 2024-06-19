PROGRAM()

LICENSE(SMLNJ)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(20200916)

NO_UTIL()

NO_RUNTIME()

NO_COMPILER_WARNINGS()

IF (OS_WINDOWS)
    CFLAGS(
        -D_CONSOLE
    )
    # by some reason f2c crashes on Windows with default allocator
    ALLOCATOR(LF)
ENDIF()

SRCDIR(contrib/tools/f2c/src)

SRCS(
    main.c
    init.c
    gram.c
    lex.c
    proc.c
    equiv.c
    data.c
    format.c
    expr.c
    exec.c
    intr.c
    io.c
    misc.c
    error.c
    mem.c
    names.c
    output.c
    p1output.c
    pread.c
    put.c
    putpcc.c
    vax.c
    formatdata.c
    parse_args.c
    niceprintf.c
    cds.c
    sysdep.c
    version.c
)

END()
