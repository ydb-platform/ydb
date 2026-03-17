from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_circular_buffer = NixSourceProject(
    nixattr="boost_circular_buffer",
    arcdir=boost.make_arcdir("circular_buffer"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
