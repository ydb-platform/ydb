from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_predef = NixSourceProject(
    nixattr="boost_predef",
    arcdir=boost.make_arcdir("predef"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "cygwin/version.h",
    ],
    post_install=post_install,
)
