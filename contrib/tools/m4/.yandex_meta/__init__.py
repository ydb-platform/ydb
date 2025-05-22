import os
import itertools

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker import pathutil
from devtools.yamaker.project import NixProject


WINDOWS_SRCS = [
    "close.c",
    "dup2.c",
    "error.c",
    "float+.h",
    "fpending.c",
    "fseeko.c",
    "getdtablesize.c",
    "getopt.c",
    "getopt1.c",
    "getopt_int.h",
    "gettimeofday.c",
    "isnand-nolibm.h",
    "isnanf-nolibm.h",
    "isnanl-nolibm.h",
    "localeconv.c",
    "mkdtemp.c",
    "msvc-inval.c",
    "msvc-inval.h",
    "msvc-nothrow.c",
    "msvc-nothrow.h",
    "nl_langinfo.c",
    "obstack.c",
    "raise.c",
    "regex.c",
    "regex.h",
    "sigaction.c",
    "siglist.h",
    "sigprocmask.c",
    "strsignal.c",
    "w32spawn.h",
    "waitpid.c",
]

DARWIN_SRCS = [
    "error.c",
    "fpending.c",
    "obstack.c",
    "regex.c",
    "secure_getenv.c",
]

EXCESSIVE_SRCS = [
    "alloca.h",
    "asprintf.c",
    "fflush.c",
    "fpurge.c",
    "fseek.c",
    "freading.c",
    "langinfo.h",
    "limits.h",
    "locale.h",
    "math.h",
    "signal.h",
    "spawn.h",
    "stdio.h",
    "stdlib.h",
    "string.h",
    "sys/stat.h",
    "sys/time.h",
    "sys/types.h",
    "sys/wait.h",
    "time.h",
    "unistd.h",
    "wchar.h",
    "wctype.h",
]

MUSL_SRCS = [
    "error.c",
    "obstack.c",
    "regex.c",
]


def post_install(self):

    with self.yamakes["lib"] as gnulib:
        gnulib.SRCS.remove("freadahead.c")
        gnulib.SRCS.remove("fseeko.c")

        # Provide sys/random.h implementations which is used disregarding HAVE_SYS_RANDOM configuration value
        gnulib.PEERDIR.add("contrib/libs/libc_compat")

        gnulib.after(
            "SRCS",
            Switch(
                MUSL=Linkable(
                    SRCS=[src for src in MUSL_SRCS if pathutil.is_source(src)],
                ),
                OS_WINDOWS=Linkable(
                    SRCS=[src for src in WINDOWS_SRCS if pathutil.is_source(src)],
                    ADDINCL=[GLOBAL(f"{self.arcdir}/lib/platform/win64")],
                ),
                OS_DARWIN=Linkable(
                    SRCS=[src for src in DARWIN_SRCS if pathutil.is_source(src)],
                ),
            ),
        )

        gnulib.after(
            "SRCS",
            """
            IF (NOT MUSL)
                SRCS(
                    freadahead.c
                )
            ENDIF()
            """,
        )

        for src in WINDOWS_SRCS:
            if pathutil.is_source(src) and src in gnulib.SRCS:
                gnulib.SRCS.remove(src)

        for src in EXCESSIVE_SRCS:
            os.remove(f"{self.dstdir}/lib/{src}")
            if pathutil.is_source(src):
                gnulib.SRCS.remove(src)


m4 = NixProject(
    arcdir="contrib/tools/m4",
    owners=["g:cpp-contrib"],
    nixattr="m4",
    ignore_commands=[
        "bash",
        "cat",
        "sed",
    ],
    # fmt: off
    copy_sources=[
        # these are included from regex.c and should not be added into SRCS
        "lib/intprops.h",
        "lib/regcomp.c",
        "lib/regex_internal.c",
        "lib/regex_internal.h",
        "lib/regexec.c",
    ] + [f"lib/{src}" for src in itertools.chain(WINDOWS_SRCS, DARWIN_SRCS)],
    # fmt: on
    use_full_libnames=True,
    install_targets=[
        "libm4",
        "m4",
    ],
    keep_paths=[
        "lib/platform/win64/*.h",
        "lib/platform/win64/sys/*.h",
        # Keep this for now, these were backported from the future
        "lib/fpending.c",
        "lib/freadahead.c",
        "lib/fseeko.c",
        "lib/stdio-impl.h",
    ],
    platform_dispatchers=[
        "lib/config.h",
        "lib/configmake.h",
    ],
    disable_includes=[
        "InnoTekLIBC/backend.h",
        "bits/libc-lock.h",
        "gettextP.h",
        "lc-charset-dispatch.h",
        "libc-lock.h",
        "libio/",
        "localename-table.h",
        "locale/",
        "../locale/localeinfo.h",
        "OS.h",
        "os2.h",
        "os2-spawn.h",
        "mbtowc-lock.h",
        "mbrtowc-impl.h",
        "relocatable.h",
        "sigsegv.h",
        "streq.h",
        "synch.h",
        "sys/ps.h",
        "sys/single_threaded.h",
        "unistring-notinline.h",
        "vasprintf.h",
    ],
    put={
        "libm4": "lib",
        "m4": ".",
    },
    post_install=post_install,
)

m4.copy_top_sources_except |= {
    "ABOUT-NLS",
    "ChangeLog",
    "ChangeLog-2014",
}
