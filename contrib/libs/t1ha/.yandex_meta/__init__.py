from devtools.yamaker.project import GNUMakeNixProject
from devtools.yamaker.modules import Switch, Linkable


def post_install(self):
    with self.yamakes["."] as t1ha:
        t1ha.after("SRCS", Switch(ARCH_X86_64=Linkable(CFLAGS=["-maes"])))


t1ha = GNUMakeNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/libs/t1ha",
    nixattr="t1ha",
    makeflags=["libt1ha.so"],
    disable_includes=[
        "sys/isa_defs.h",
    ],
    post_install=post_install,
)
