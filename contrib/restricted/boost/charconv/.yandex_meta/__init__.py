from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_charconv = NixSourceProject(
    nixattr="boost_charconv",
    arcdir=boost.make_arcdir("charconv"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "stdfloat",
    ],
    post_install=post_install,
)
