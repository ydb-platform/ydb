import os

from devtools.yamaker.modules import Linkable, Switch
from devtools.yamaker.project import NixProject


# libidn contains some weird proxy headers
# intended to emulate glibc compatibility layer.
#
# This headers make use of non-standard include_next statement,
# thus breaking the MSVC build.
# Some of these files are licensed under GPL license, which is non-acceptable.
#
# They will be removed during post_install
GLIBC_EMULATION_HEADERS = (
    "gl/fcntl.h",
    "gl/limits.h",
    "gl/progname.h",
    "gl/stddef.h",
    "gl/stdio.h",
    "gl/stdlib.h",
    "gl/string.h",
    "gl/sys/stat.h",
    "gl/sys/types.h",
    "gl/time.h",
    "gl/version-etc.h",
    "gl/unistd.h",
    "lib/gl/limits.h",
    "lib/gl/langinfo.h",
    "lib/gl/stddef.h",
    "lib/gl/stdlib.h",
    "lib/gl/string.h",
    "lib/gl/sys/types.h",
    "lib/gl/unistd.h",
)


GLIBC_EMULATION_SOURCES = (
    "gl/cloexec.c",
    "gl/fcntl.c",
    "gl/unistd.c",
    "gl/progname.c",
    "gl/version-etc.c",
    "lib/gl/unistd.c",
)


LINUX_SPECIFIC_SRCS = [
    "gl/getprogname.c",
]

WINDOWS_SPECIFIC_SRCS = [
    "gl/getprogname.c",
    "lib/gl/strverscmp.c",
]


def post_install(self):
    # libidn.map bears GPL-3.0 license and thus can not be used
    os.remove(f"{self.dstdir}/lib/libidn.map")

    with self.yamakes["static"] as libidn:
        # Drop the stuff that breaks Windows build
        for src in GLIBC_EMULATION_HEADERS:
            os.remove(f"{self.dstdir}/{src}")
        for src in GLIBC_EMULATION_SOURCES:
            os.remove(f"{self.dstdir}/{src}")
            libidn.SRCS.remove(src)

        # Support Darwin.
        for src in LINUX_SPECIFIC_SRCS:
            libidn.SRCS.remove(src)
        libidn.after(
            "SRCS",
            Switch(
                OS_LINUX=Linkable(SRCS=LINUX_SPECIFIC_SRCS),
                OS_WINDOWS=Linkable(SRCS=WINDOWS_SPECIFIC_SRCS),
            ),
        )

    self.make_dll_dispatcher(
        switch_flag="USE_IDN",
        switch_as_enum=True,
        handle_local=True,
        default_local_flags={
            "CFLAGS": ("USE_LOCAL_IDN_CFLAGS",),
            "LDFLAGS": ("USE_LOCAL_IDN_LDFLAGS", "-lidn"),
        },
        exports_script="libidn.exports",
        before_switch="""
            IF(EXPORT_CMAKE)
                OPENSOURCE_EXPORT_REPLACEMENT(
                    CMAKE IDN
                    CMAKE_TARGET IDN::IDN
                )
            ENDIF()

            """,
        or_local_condition="EXPORT_CMAKE",
    )


libidn = NixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/libidn",
    nixattr="libidn",
    ignore_commands=["bash", "sed", "cat"],
    inclink={
        "include": [
            "lib/idn-free.h",
            "lib/idn-int.h",
            "lib/idna.h",
            "lib/pr29.h",
            "lib/punycode.h",
            "lib/stringprep.h",
            "lib/tld.h",
        ]
    },
    addincl_global={"static": {"../include"}},
    install_targets=[
        "libidn",
        "libgnu",
    ],
    put={
        "libidn": "static",
    },
    put_with={
        "libidn": {"libgnu"},
    },
    disable_includes={
        "cheri.h",
        "getopt-cdefs.h",
        "getopt-pfx-core.h",
        "msvc-nothrow.h",
        "random.h",
        "sys/ps.h",
        # sys/random.h is not (yet) present in current version of glibc
        # just disable it, as it is not included anyway
        "sys/random.h",
        "unistring-notinline.h",
    },
    platform_dispatchers=("config.h",),
    keep_paths=["dynamic/libidn.exports"],
    copy_sources=("lib/gl/strverscmp.c", "lib/gl/libc-config.h", "lib/gl/cdefs.h"),
    post_install=post_install,
    use_full_libnames=True,
)
