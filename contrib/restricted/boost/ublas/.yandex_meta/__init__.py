from devtools.yamaker import boost
from devtools.yamaker.project import NixSourceProject


def post_install(self):
    self.yamakes["."] = boost.make_library(self)


boost_ublas = NixSourceProject(
    nixattr="boost_ublas",
    arcdir=boost.make_arcdir("ublas"),
    owners=["g:cpp-contrib"],
    copy_sources=[
        "include/boost/",
    ],
    disable_includes=[
        "boost/numeric/bindings/*",
    ],
    post_install=post_install,
)
