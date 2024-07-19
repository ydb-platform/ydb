LIBRARY()

LICENSE(
    BSD-3-Clause AND
    GPL-3.0-or-later
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

IF (NOT MUSL)
    NO_RUNTIME()
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/tools/bison/lib
)

IF (OS_WINDOWS)
    ADDINCL(
        GLOBAL contrib/tools/bison/lib/platform/win64
    )
ENDIF()

IF (NOT OS_WINDOWS)
    CFLAGS(
        GLOBAL -Dregerror=gnu_regerror
        GLOBAL -Dregfree=gnu_regfree
        GLOBAL -Dregexec=gnu_regexec
    )
ENDIF()

SRCS(
    abitset.c
    argmatch.c
    basename-lgpl.c
    basename.c
    binary-io.c
    bitrotate.c
    bitset.c
    bitset_stats.c
    bitsetv-print.c
    bitsetv.c
    c-ctype.c
    c-strcasecmp.c
    c-strncasecmp.c
    cloexec.c
    close.c
    close-stream.c
    closeout.c
    concat-filename.c
    dirname-lgpl.c
    dirname.c
    dup-safer-flag.c
    dup-safer.c
    dup2.c
    ebitset.c
    error.c
    exitfail.c
    fatal-signal.c
    fd-hook.c
    fd-safer-flag.c
    fd-safer.c
    fopen-safer.c
    get-errno.c
    getdtablesize.c
    hash.c
    isnanl.c
    isnand.c
    lbitset.c
    localcharset.c
    mbswidth.c
    pipe-safer.c
    pipe2-safer.c
    printf-args.c
    printf-frexp.c
    printf-frexpl.c
    printf-parse.c
    progname.c
    quotearg.c
    sig-handler.c
    spawn-pipe.c
    stpcpy.c
    stripslash.c
    timevar.c
    unistd.c
    vasnprintf.c
    vbitset.c
    wait-process.c
    wctype-h.c
    xalloc-die.c
    xconcat-filename.c
    xmalloc.c
    xmemdup0.c
    xsize.c
    xstrndup.c
)

IF (NOT MUSL)
    SRCS(
        fseterr.c
    )
ENDIF()

IF (NOT OS_LINUX)
    SRCS(
        pipe2.c
        strverscmp.c
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        fcntl.c
        getopt.c
        getopt1.c
        msvc-inval.c
        msvc-nothrow.c
        open.c
        raise.c
        sigaction.c
        sigprocmask.c
        strndup.c
        waitpid.c
        wcwidth.c
        uniwidth/width.c
    )
ENDIF()

IF (NOT OS_LINUX OR MUSL)
    SRCS(
        obstack.c
        obstack_printf.c
    )
ENDIF()

IF (OS_CYGWIN OR OS_LINUX)
    #not need it
ELSE()
    SRCS(
        fpending.c
    )
ENDIF()

END()
