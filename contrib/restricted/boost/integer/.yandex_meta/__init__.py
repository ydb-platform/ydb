from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_integer = NixSourceProject(
    nixattr="boost_integer",
    arcdir=boost.make_arcdir("integer"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
