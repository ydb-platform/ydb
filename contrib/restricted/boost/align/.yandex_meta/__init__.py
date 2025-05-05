from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_align = NixSourceProject(
    nixattr="boost_align",
    arcdir=boost.make_arcdir("align"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
