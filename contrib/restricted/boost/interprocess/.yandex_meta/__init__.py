from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_interprocess = NixSourceProject(
    nixattr="boost_interprocess",
    arcdir=boost.make_arcdir("interprocess"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "atomic.h",
        "vxAtomicLib.h",
        "vxCpuLib.h",
    ],
    post_install=post_install,
)
