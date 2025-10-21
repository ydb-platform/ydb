from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_throw_exception = NixSourceProject(
    nixattr="boost_throw_exception",
    arcdir=boost.make_arcdir("throw_exception"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    post_install=post_install,
)
