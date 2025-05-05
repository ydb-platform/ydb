from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_functional = NixSourceProject(
    nixattr="boost_functional",
    arcdir=boost.make_arcdir("functional"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
