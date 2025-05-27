import os.path as P
import shutil

from devtools.yamaker.modules import GLOBAL, Switch, Linkable
from devtools.yamaker.project import GNUMakeNixProject


def post_build(self):
    for name in "expat.h", "expat_external.h":
        shutil.move(P.join(self.dstdir, "lib", name), P.join(self.dstdir, name))

    with self.yamakes["."] as expat:
        expat.CFLAGS.remove("-DXML_ENABLE_VISIBILITY=1")
        expat.after(
            "CFLAGS",
            Switch(
                OS_WINDOWS=Linkable(CFLAGS=[GLOBAL("-DXML_STATIC")]),
            ),
        )


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
        "lib/winconfig.h",
    ],
    install_targets=[
        "expat",
    ],
    post_build=post_build,
)
