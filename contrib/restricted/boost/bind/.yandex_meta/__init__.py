from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_bind = NixSourceProject(
    nixattr="boost_bind",
    arcdir=boost.make_arcdir("bind"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
