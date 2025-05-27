from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_pool = NixSourceProject(
    nixattr="boost_pool",
    arcdir=boost.make_arcdir("pool"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    copy_sources_except_ext=["*.bat", "*.m4", "*.sh"],
    post_install=post_install,
)
