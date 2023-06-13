LIBRARY()

LICENSE(
    BSD-3-Clause AND
    GPL-3.0-or-later AND
    LGPL-2.0-or-later
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

IF (NOT MUSL)
    NO_RUNTIME()
ENDIF()

NO_COMPILER_WARNINGS()

ADDINCL(
    GLOBAL contrib/tools/bison/gnulib/src
)

IF (OS_WINDOWS)
    ADDINCL(
        GLOBAL contrib/tools/bison/gnulib/platform/win64
    )
ELSE()
    ADDINCL(
        GLOBAL contrib/tools/bison/gnulib/platform/posix
    )
ENDIF()

IF (OS_DARWIN)
    CFLAGS(
        -D_XOPEN_SOURCE=600
    )
ENDIF()

IF (NOT OS_WINDOWS)
    CFLAGS(
        GLOBAL -Dregcomp=gnu_regcomp
        GLOBAL -Dregerror=gnu_regerror
        GLOBAL -Dregfree=gnu_regfree
        GLOBAL -Dregexec=gnu_regexec
    )
ENDIF()

SRCS(
    src/abitset.c
    src/argmatch.c
    src/asnprintf.c
    src/basename-lgpl.c
    src/basename.c
    src/binary-io.c
    src/bitrotate.c
    src/bitset.c
    src/bitset_stats.c
    src/bitsetv-print.c
    src/bitsetv.c
    src/c-ctype.c
    src/c-stack.c
    src/c-strcasecmp.c
    src/c-strncasecmp.c
    src/calloc.c
    src/clean-temp.c
    src/cloexec.c
    src/close-stream.c
    src/close.c
    src/closein.c
    src/closeout.c
    src/concat-filename.c
    src/dirname-lgpl.c
    src/dirname.c
    src/dup-safer-flag.c
    src/dup-safer.c
    src/dup2.c
    src/ebitset.c
    src/error.c
    src/execute.c
    src/exitfail.c
    src/fatal-signal.c
    src/fclose.c
    src/fcntl.c
    src/fd-hook.c
    src/fd-safer-flag.c
    src/fd-safer.c
    src/fflush.c
    src/filenamecat-lgpl.c
    src/filenamecat.c
    src/float.c
    src/fopen-safer.c
    src/fpurge.c
    src/freading.c
    src/fstat.c
    src/get-errno.c
    src/getdtablesize.c
    src/getopt.c
    src/getopt1.c
    src/gl_avltree_oset.c
    src/gl_linkedhash_list.c
    src/gl_list.c
    src/gl_oset.c
    src/gl_xlist.c
    src/gl_xoset.c
    src/hash.c
    src/isnand.c
    src/isnanf.c
    src/isnanl.c
    src/itold.c
    src/lbitset.c
    src/localcharset.c
    src/lseek.c
    src/lstat.c
    src/malloc.c
    src/malloca.c
    src/mbrtowc.c
    src/mbswidth.c
    src/memchr2.c
    src/mkstemp-safer.c
    src/nl_langinfo.c
    src/pipe-safer.c
    src/pipe2-safer.c
    src/printf-args.c
    src/printf-frexp.c
    src/printf-frexpl.c
    src/printf-parse.c
    src/progname.c
    src/quotearg.c
    src/raise.c
    src/rawmemchr.c
    src/readlink.c
    src/realloc.c
    src/regex.c
    src/rename.c
    src/rmdir.c
    src/secure_getenv.c
    src/sig-handler.c
    src/signbitd.c
    src/signbitf.c
    src/signbitl.c
    src/spawn-pipe.c
    src/stat.c
    src/stpcpy.c
    src/strchrnul.c
    src/strdup.c
    src/stripslash.c
    src/tempname.c
    src/timevar.c
    src/tmpdir.c
    src/unistd.c
    src/unsetenv.c
    src/vasnprintf.c
    src/vbitset.c
    src/verror.c
    src/version-etc-fsf.c
    src/version-etc.c
    src/wait-process.c
    src/wctype-h.c
    src/xalloc-die.c
    src/xasprintf.c
    src/xconcat-filename.c
    src/xmalloc.c
    src/xmalloca.c
    src/xmemdup0.c
    src/xprintf.c
    src/xsize.c
    src/xstrndup.c
    src/xvasprintf.c
)

IF (NOT MUSL)
    SRCS(
        src/freadahead.c
        src/fseterr.c
        #        src/fseek.c
    )
ENDIF()

IF (NOT OS_LINUX)
    SRCS(
        src/pipe2.c
        src/strverscmp.c
    )
ENDIF()

IF (NOT OS_WINDOWS)
    SRCS(
        src/stdio-write.c
    )
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        src/frexp.c
        src/wcrtomb.c
        src/perror.c
        src/strstr.c
        src/mkstemp.c
        src/vasprintf.c
        src/strsignal.c
        src/mkdtemp.c
        src/fseeko.c
        src/fopen.c
        src/ftello.c
        src/gettimeofday.c
        src/localeconv.c
        src/msvc-inval.c
        src/msvc-nothrow.c
        src/open.c
        src/sigaction.c
        src/sigprocmask.c
        src/snprintf.c
        src/spawn_faction_addclose.c
        src/spawn_faction_adddup2.c
        src/spawn_faction_addopen.c
        src/spawn_faction_destroy.c
        src/spawn_faction_init.c
        src/spawnattr_destroy.c
        src/spawnattr_init.c
        src/spawnattr_setflags.c
        src/spawnattr_setsigmask.c
        src/spawni.c
        src/spawnp.c
        src/strndup.c
        src/waitpid.c
        src/wcwidth.c
        src/uniwidth/width.c
    )
ENDIF()

IF (NOT OS_LINUX OR MUSL)
    SRCS(
        src/obstack.c
        src/obstack_printf.c
    )
ENDIF()

IF (OS_CYGWIN OR OS_LINUX)
    #not need it
ELSE()
    SRCS(
        src/fpending.c
    )
ENDIF()

END()
