from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_dynamic_bitset = NixSourceProject(
    nixattr="boost_dynamic_bitset",
    arcdir=boost.make_arcdir("dynamic_bitset"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "iostream.h",
    ],
    post_install=post_install,
)
