from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_function = NixSourceProject(
    nixattr="boost_function",
    arcdir=boost.make_arcdir("function"),
    owners=["g:cpp-contrib", "g:taxi-common"],
    copy_sources=[
        "include/boost/",
    ],
    copy_sources_except_ext=["*.pl"],
    post_install=post_install,
)
