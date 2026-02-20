import os
import itertools

from devtools.yamaker.modules import GLOBAL, Linkable, Switch
from devtools.yamaker import pathutil
from devtools.yamaker.project import NixProject


WINDOWS_SRCS = [
    "alloca.h",
    "close.c",
    "dup2.c",
    "error.c",
    "fcntl.c",
    "fd-hook.c",
    "fd-hook.h",
    "float+.h",
    "fpending.c",
    "getdtablesize.c",
    "getopt.c",
    "getopt1.c",
    "getopt_int.h",
    "isnand-nolibm.h",
    "isnanf-nolibm.h",
    "isnanl-nolibm.h",
    "msvc-inval.c",
    "msvc-inval.h",
    "msvc-nothrow.c",
    "msvc-nothrow.h",
    "obstack.c",
    "obstack_printf.c",
    "open.c",
    "raise.c",
    "sigaction.c",
    "sigprocmask.c",
    "stpcpy.c",
    "stpncpy.c",
    "strndup.c",
    "strverscmp.c",
    "unitypes.h",
    "uniwisth.h",
    "w32spawn.h",
    "waitpid.c",
    "wcwidth.c",
    "windows-initguard.h",
    "windows-mutex.c",
    "windows-mutex.h",
    "windows-once.c",
    "windows-once.h",
    "windows-recmutex.c",
    "windows-recmutex.h",
    "windows-rwlock.c",
    "windows-rwlock.h",
    "windows-tls.c",
    "windows-tls.h",
]

MUSL_SRCS = [
    "error.c",
    "obstack.c",
    "obstack_printf.c",
]

DARWIN_SRCS = [
    "error.c",
    "fpending.c",
    "obstack.c",
    "obstack_printf.c",
    "strverscmp.c",
]

EXCESSIVE_SRCS = [
    "alloca.h",
    "fcntl.h",
    "fprintf.c",
    "gettime.c",
    "iconv.h",
    "inttypes.h",
    "math.h",
    "limits.h",
    "locale.h",
    "printf.c",
    "sched.h",
    "signal.h",
    "snprintf.c",
    "spawn.h",
    "sprintf.c",
    "stdio.h",
    "stdlib.h",
    "string.h",
    "sys/ioctl.h",
    "sys/resource.h",
    "sys/time.h",
    "sys/times.h",
    "sys/types.h",
    "sys/wait.h",
    "termios.h",
    "time.h",
    "unistd.h",
    "vfprintf.c",
    "vsnprintf.c",
    "vsprintf.c",
    "wchar.h",
    "wctype.h",
]


def post_install(self):
    with self.yamakes["."] as yamake:
        yamake.after(
            "CFLAGS",
            """
IF (OPENSOURCE)
    LDFLAGS(-Wl,--allow-multiple-definition)
ENDIF()
            """,
        )

    with self.yamakes["lib"] as gnulib:
        # musl-libc has fseterr
        gnulib.SRCS.remove("fseterr.c")
        gnulib.after(
            "SRCS",
            """
            IF (NOT MUSL)
                SRCS(
                    fseterr.c
                )
            ENDIF()
            """,
        )
        gnulib.after(
            "SRCS",
            Switch(
                MUSL=Linkable(
                    SRCS=MUSL_SRCS,
                ),
                OS_DARWIN=Linkable(
                    SRCS=DARWIN_SRCS,
                ),
                OS_WINDOWS=Linkable(
                    SRCS=[src for src in WINDOWS_SRCS if pathutil.is_source(src)],
                    ADDINCL=[GLOBAL(f"{self.arcdir}/lib/platform/win64")],
                ),
            ),
        )
        for src in WINDOWS_SRCS:
            if pathutil.is_source(src) and src in gnulib.SRCS:
                gnulib.SRCS.remove(src)

        for src in EXCESSIVE_SRCS:
            os.remove(f"{self.dstdir}/lib/{src}")
            if pathutil.is_source(src):
                gnulib.SRCS.remove(src)


bison = NixProject(
    arcdir="contrib/tools/bison",
    owners=["g:contrib"],
    nixattr="bison",
    ignore_commands=[
        "bash",
        "cat",
        "sed",
    ],
    use_full_libnames=True,
    install_targets=[
        "bison",
        "libbison",
    ],
    put={
        "bison": ".",
        "libbison": "lib",
    },
    keep_paths=[
        "lib/platform/win64/*.h",
        "lib/platform/win64/sys/*.h",
        # Keep this for now as upstream code crashes on Windows
        "src/scan-skel.c",
    ],
    copy_sources=[
        "data/skeletons/*.c",
        "data/skeletons/*.cc",
        "data/skeletons/*.hh",
        "data/skeletons/*.m4",
        "data/m4sugar/*.m4",
        # These lex / bison sources will not be used
        # (how does one bootstrap bison without having bison?)
        #
        # Just copy them for informational purposes
        "src/scan-code.l",
        "src/scan-gram.l",
        "src/scan-skel.l",
        "src/parse-gram.y",
    ]
    + [f"lib/{src}" for src in itertools.chain(MUSL_SRCS, DARWIN_SRCS, WINDOWS_SRCS)],
    copy_sources_except=[
        # Don't need them for now, reduce import footprint
        "data/skeletons/d.m4",
        "data/skeletons/d-skel.m4",
        "data/skeletons/glr.c",
        "data/skeletons/java.m4",
        "data/skeletons/java-skel.m4",
        "data/skeletons/traceon.m4",
    ],
    platform_dispatchers=[
        "lib/config.h",
        "lib/configmake.h",
    ],
    disable_includes=[
        "InnoTekLIBC/backend.h",
        "bits/libc-lock.h",
        "libio/",
        "synch.h",
        "random.h",
        "OS.h",
        "os2.h",
        "sys/ps.h",
        "sys/ptem.h",
        "sys/single_threaded.h",
        "mbtowc-lock.h",
        "mbrtowc-impl.h",
        "lc-charset-dispatch.h",
        "unistring-notinline.h",
        "vasprintf.h",
    ],
    post_install=post_install,
)

bison.copy_top_sources_except |= {
    "ABOUT-NLS",
    "ChangeLog",
    "ChangeLog-2012",
    "ChangeLog-1998",
    "INSTALL",
}
