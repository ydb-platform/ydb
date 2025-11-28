from devtools.yamaker.project import CMakeNinjaNixProject
from devtools.yamaker.modules import GLOBAL


def post_install(self):
    with self.yamakes["."] as m:
        for i in range(len(m.CFLAGS)):
            if m.CFLAGS[i] == "-DREPLXX_STATIC":
                m.CFLAGS[i] = GLOBAL(m.CFLAGS[i])


replxx = CMakeNinjaNixProject(
    owners=["g:cpp-contrib"],
    arcdir="contrib/restricted/patched/replxx",
    nixattr="yamaker-replxx",
    copy_sources=[
        "src/windows.hxx",
    ],
    post_install=post_install,
)
