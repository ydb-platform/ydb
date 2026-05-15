import os.path as P
import shutil

from devtools.yamaker import fileutil
from devtools.yamaker.modules import GLOBAL, Switch, Linkable
from devtools.yamaker.project import GNUMakeNixProject


WINDOWS_RANDOM_SRCS = [
    "lib/random_rand_s.c",
]


def post_build(self):
    for name in "expat.h", "expat_external.h":
        shutil.move(P.join(self.dstdir, "lib", name), P.join(self.dstdir, name))

    with self.yamakes["."] as expat:
        POSIX_RANDOM_SRCS = []
        IOS_RANDOM_SRCS = []
        for src in fileutil.files(self.dstdir, rel=True):
            if src in WINDOWS_RANDOM_SRCS:
                continue
            if src.startswith("lib/random_") and src.endswith(".c"):
                expat.SRCS.remove(src)
                POSIX_RANDOM_SRCS.append(src)
                if "arc4random" in src:
                    IOS_RANDOM_SRCS.append(src)

        expat.CFLAGS.remove("-DXML_ENABLE_VISIBILITY=1")
        expat.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(
		    CFLAGS=[GLOBAL("-DXML_STATIC")],
                    SRCS=WINDOWS_RANDOM_SRCS,
                ),
                OS_IOS=Linkable(
                    SRCS=IOS_RANDOM_SRCS,
                ),
                default=Linkable(
                    SRCS=POSIX_RANDOM_SRCS,
                ),
            ),
        )
        expat.PEERDIR.add("contrib/libs/libc_compat")


expat = GNUMakeNixProject(
    arcdir="contrib/libs/expat",
    nixattr="expat",
    makeflags=["-C", "lib", "libexpat.la"],
    disable_includes=[
        "watcomconfig.h",
        "bsd/stdlib.h",
        "proto/expat.h",
    ],
    copy_sources=[
        "Changes",
        "lib/random_rand_s.c",
        "lib/random_rand_s.h",
        "lib/winconfig.h",
    ],
    install_targets=[
        "expat",
    ],
    platform_dispatchers=[
        "expat_config.h",
    ],
    post_build=post_build,
)
