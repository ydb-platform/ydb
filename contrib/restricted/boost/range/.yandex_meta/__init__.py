from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_range = NixSourceProject(
    nixattr="boost_range",
    arcdir=boost.make_arcdir("range"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "ostream.h",
        "xutility",
    ],
    post_install=post_install,
)
