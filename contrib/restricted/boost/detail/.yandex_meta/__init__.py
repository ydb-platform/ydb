from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_detail = NixSourceProject(
    nixattr="boost_detail",
    arcdir=boost.make_arcdir("detail"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        # if defined(__MINGW32__)
        "../include/fenv.h",
    ],
    post_install=post_install,
)
